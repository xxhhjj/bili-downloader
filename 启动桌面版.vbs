Option Explicit
' BiliTools-Lite desktop launcher - starts desktop.js without a console window
Dim sh, fso, root, cmd
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
Set sh = CreateObject("WScript.Shell")
sh.CurrentDirectory = root
cmd = "node """ & root & "\desktop.js"""
sh.Run cmd, 0, False
