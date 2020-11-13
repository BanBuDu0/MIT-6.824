package raft

import "log"

// Debugging
const Debug = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 2 {
		log.Printf(format, a...)
	}
	return
}

func BPrintf(format string, a ...interface{}) {
	if Debug == 3 {
		log.Printf(format, a...)
	}
	return
}
