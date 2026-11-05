use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Mutex;
use tauri::{Manager, WebviewUrl, WebviewWindowBuilder, WindowEvent};

/// 持有 Node 后端子进程，窗口关闭时统一回收（server.js 会自行结束 aria2c）。
struct ServerChild(Mutex<Option<Child>>);

/// 定位项目根目录：
///   - 开发态：CARGO_MANIFEST_DIR 的上级（含 server.js / public）
///   - 打包态：Tauri resource 目录（bundle.resources 已把 server.js 与 public 放进来）
///   - 可用 BILITOOLS_ROOT 环境变量覆盖
fn locate_root(app: &tauri::App) -> PathBuf {
    if let Ok(p) = std::env::var("BILITOOLS_ROOT") {
        return PathBuf::from(p);
    }
    if let Ok(m) = std::env::var("CARGO_MANIFEST_DIR") {
        if let Some(parent) = PathBuf::from(m).parent() {
            let candidate = parent.to_path_buf();
            if candidate.join("server.js").exists() {
                return candidate;
            }
        }
    }
    if let Ok(res) = app.path().resource_dir() {
        if res.join("server.js").exists() {
            return res;
        }
        if let Some(parent) = res.parent() {
            if parent.join("server.js").exists() {
                return parent.to_path_buf();
            }
        }
    }
    PathBuf::from(".")
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let root = locate_root(app);
            let mut cmd = Command::new("node");
            cmd.arg(root.join("server.js"))
                .current_dir(&root)
                .env("PORT", "3000");

            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
            }

            let child = cmd.spawn().map_err(|e| {
                std::io::Error::other(format!(
                    "无法启动 Node 后端（需要 Node.js 22+，且 server.js 位于 {:?}）: {}",
                    root, e
                ))
            })?;
            app.manage(ServerChild(Mutex::new(Some(child))));

            // 等后端监听就绪，避免首屏连接失败
            for _ in 0..50 {
                if std::net::TcpStream::connect("127.0.0.1:3000").is_ok() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            // 用外部 URL 建窗：页面由本机 Node 服务提供，Rust 只负责桌面壳与进程生命周期
            let url = "http://127.0.0.1:3000/"
                .parse()
                .expect("valid backend URL");
            WebviewWindowBuilder::new(app, "main", WebviewUrl::External(url))
                .title("BiliTools-Lite · B 站资源下载")
                .inner_size(1180.0, 880.0)
                .min_inner_size(900.0, 640.0)
                .center()
                .build()?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::Destroyed = event {
                if let Some(state) = window.app_handle().try_state::<ServerChild>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(mut child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("启动 BiliTools-Lite 失败");
}
