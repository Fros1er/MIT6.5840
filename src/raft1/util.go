package raft

import (
	"log"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

//const FuncTrace = true

const FuncTrace = false

func FTracef(format string, a ...interface{}) {
	if FuncTrace {
		log.Printf(format, a...)
	}
}

func todo() {
	panic("Not implemented")
}

func assert(assertion bool, msg string) {
	if !assertion {
		panic(msg)
	}
}

func assertf(assertion bool, format string, a ...interface{}) {
	if !assertion {
		log.Panicf(format, a...)
	}
}

func mutexLocked(m *sync.Mutex) bool {
	notLocked := m.TryLock()
	if notLocked {
		m.Unlock()
	}
	return !notLocked
	//state := reflect.ValueOf(m).Elem().FieldByName("state")
	//return state.Int()&1 == 1
}

func If[T any](cond bool, vtrue T, vfalse func() T) T {
	if cond {
		return vtrue
	}
	return vfalse()
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
