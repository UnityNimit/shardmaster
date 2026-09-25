//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	enableVirtualTerminalProcessing uint32 = 0x0004
	cpUTF8                          uintptr = 65001
)

func initNativeConsole() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")

	// 1. Enable UTF-8 code page (65001) so box borders render cleanly in conhost.exe and cmd.exe
	setConsoleOutputCP := kernel32.NewProc("SetConsoleOutputCP")
	setConsoleCP := kernel32.NewProc("SetConsoleCP")
	_, _, _ = setConsoleOutputCP.Call(cpUTF8)
	_, _, _ = setConsoleCP.Call(cpUTF8)

	// 2. Enable ANSI Virtual Terminal Processing on Stdout and Stderr for double-click launches
	getConsoleMode := kernel32.NewProc("GetConsoleMode")
	setConsoleMode := kernel32.NewProc("SetConsoleMode")

	for _, f := range []*os.File{os.Stdout, os.Stderr} {
		if f == nil {
			continue
		}
		h := syscall.Handle(f.Fd())
		var mode uint32
		r1, _, _ := getConsoleMode.Call(uintptr(h), uintptr(unsafe.Pointer(&mode)))
		if r1 != 0 {
			_, _, _ = setConsoleMode.Call(uintptr(h), uintptr(mode|enableVirtualTerminalProcessing))
		}
	}

	// 3. Set a clean window title when launched by double-clicking in Windows Explorer
	setConsoleTitleW := kernel32.NewProc("SetConsoleTitleW")
	if titlePtr, err := syscall.UTF16PtrFromString("ShardMaster v2.0 - Unified Interactive Control Center (50,000,000 Rows)"); err == nil {
		_, _, _ = setConsoleTitleW.Call(uintptr(unsafe.Pointer(titlePtr)))
	}
}
