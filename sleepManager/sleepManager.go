package sleepManager

import "time"
import "fmt"

type Event int
type EventChannel chan Event
type SleepDurationChannel chan uint
type SleepDurationRequestChannel chan SleepDurationChannel

const (
	SUCCESS Event = iota
	ERROR
)

type SleepManager struct {
	EventChannel         EventChannel
	sleepDuration        uint
	sleepDurationMax     uint
	sleepDurationInitial uint
	sleepNow             SleepDurationRequestChannel // reports how long to sleep now
	quitChannel          chan interface{}            // channel used to signal when to exit
}

func NewSleepManager(initial uint, max uint) *SleepManager {
	ans := new(SleepManager)
	ans.EventChannel = make(EventChannel)
	ans.sleepDurationInitial = initial
	ans.sleepDurationMax = max
	ans.sleepDuration = initial
	ans.sleepNow = make(SleepDurationRequestChannel)
	ans.quitChannel = make(chan interface{})

	go func() {
		for {
			select {
			case event := <-ans.EventChannel:
				switch event {
				case SUCCESS:
					ans.sleepDuration = ans.sleepDurationInitial
				case ERROR:
					ans.sleepDuration <<= 1 // sleep twice as long by bit-shifting 1
					if ans.sleepDuration > ans.sleepDurationMax {
						ans.sleepDuration = ans.sleepDurationMax
					}
				}
			case n := <-ans.sleepNow:
				n <- ans.sleepDuration
			case <-ans.quitChannel:
				return
			}
		}
	}()

	return ans
}

func (sm *SleepManager) Shutdown() {
	close(sm.quitChannel)
}

func (sm *SleepManager) Error() {
	sm.EventChannel <- ERROR
}

func (sm *SleepManager) Success() {
	sm.EventChannel <- SUCCESS
}

func (sm *SleepManager) Sleep() {
	howLong := make(SleepDurationChannel) // a response channel containing ans.sleepDuration
	// remove howLong channel
	defer close(howLong)

	// ask for the current sleep duration
	// by placing the howLong channel in sleepNow
	select {
	case sm.sleepNow <- howLong:
	case <-sm.quitChannel: // return if the quitChannel is closed
		return
	}

	// block until duration (found by reading howLong channel) has expired
	dur := <-howLong
	fmt.Printf("sleeping for %+v second(s)...\n", dur)
	select {
	case <-time.After(time.Duration(dur) * time.Second): // either time will elapse
	case <-sm.quitChannel: // or, our quitChannel will be closed
	}
}
