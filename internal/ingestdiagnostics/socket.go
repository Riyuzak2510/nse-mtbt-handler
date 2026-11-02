package ingestdiagnostics

import (
	"net"
	"syscall"
)

func readBuffer(conn *net.UDPConn) (int, error) {
	raw, e := conn.SyscallConn()
	if e != nil {
		return 0, e
	}
	var size int
	var err error
	e = raw.Control(func(fd uintptr) { size, err = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF) })
	if e != nil {
		return 0, e
	}
	return size, err
}
