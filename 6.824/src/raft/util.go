package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}

func BPrintf(format string, a ...interface{}) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	return
}
