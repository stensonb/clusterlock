package sleepManager

import (
	"math"
	"testing"
	"time"
)

func diff(t1, t2 time.Time) uint {
	// round the diff down to seconds -- this could caused failed tests on slow systems
	return uint(math.Abs(float64(t1.Sub(t2).Seconds())))
}

func TestSleepManager(t *testing.T) {
	min := uint(1)
	max := uint(60)
	sm := NewSleepManager(min, max)

	time0 := time.Now()
	sm.Sleep()
	time1 := time.Now()
	diff := diff(time0, time1)

	if diff != min {
		t.Error("didn't sleep for long enough")
	}
}

func TestErrorCondition(t *testing.T) {
	min := uint(1)
	max := uint(60)
	sm := NewSleepManager(min, max)
	errors := 2

	for i := 0; i < errors; i++ {
		sm.Error()
	}

	time0 := time.Now()
	sm.Sleep()
	time1 := time.Now()
	diff := diff(time0, time1)

	if diff != (min * uint(math.Pow(float64(2), float64(errors)))) {
		t.Error("didn't sleep for long enough")
	}
}

func TestMaxSleep(t *testing.T) {
	min := uint(1)
	max := uint(2)
	sm := NewSleepManager(min, max)
	errors := 5

	// make lots of errors
	for i := 0; i < errors; i++ {
		sm.Error()
	}

	time0 := time.Now()
	sm.Sleep()
	time1 := time.Now()
	diff := diff(time0, time1)

	if diff != max {
		t.Error("slept for too long: ", diff)
	}
}

func TestSuccess(t *testing.T) {
	min := uint(1)
	max := uint(60)
	sm := NewSleepManager(min, max)
	errors := 5

	// make lots of errors
	for i := 0; i < errors; i++ {
		sm.Error()
	}

	// call success() to wipe out errors
	sm.Success()

	time0 := time.Now()
	sm.Sleep()
	time1 := time.Now()
	diff := diff(time0, time1)

	if diff != min {
		t.Error("slept for too long: ", diff)
	}
}

func TestOpenClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		sm := NewSleepManager(1, 60)
		sm.Shutdown()
	}
}

func TestInterruptSleep(t *testing.T) {
	min := uint(1)
	max := uint(60)
	sm := NewSleepManager(min, max)
	errors := 10

	// make lots of errors
	for i := 0; i < errors; i++ {
		sm.Error()
	}

	sm.Shutdown()

	time0 := time.Now()
	sm.Sleep()
	time1 := time.Now()
	diff := diff(time0, time1)

	if diff != 0 {
		t.Error("sleep interrupt didn't work")
	}

}
