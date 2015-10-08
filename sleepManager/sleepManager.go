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
}

func NewSleepManager(initial, max uint) *SleepManager {
	ans := new(SleepManager)
	ans.EventChannel = make(EventChannel)
	ans.sleepDurationInitial = initial
	ans.sleepDurationMax = max
	ans.sleepDuration = initial
	ans.sleepNow = make(SleepDurationRequestChannel)

	go func() {
		for {
			select {
			case event := <-ans.EventChannel:
				switch event {
				case SUCCESS:
					ans.sleepDuration = ans.sleepDurationInitial
				case ERROR:
					ans.sleepDuration = ans.sleepDuration << 1 // sleep twice as long
					if ans.sleepDuration > ans.sleepDurationMax {
						ans.sleepDuration = ans.sleepDurationMax
					}
				}
			case n := <-ans.sleepNow:
				n <- ans.sleepDuration
			}
		}
	}()

	return ans
}

func (sm *SleepManager) Error() {
	sm.EventChannel <- ERROR
}

func (sm *SleepManager) Success() {
	sm.EventChannel <- SUCCESS
}

func (sm *SleepManager) Sleep() {
	howLong := make(SleepDurationChannel) // a response channel containing ans.sleepDuration

	// ask for the current sleep duration
	// by placing the howLong channel in sleepNow
	sm.sleepNow <- howLong

	// block until duration (found by reading howLong channel) has expired
	dur := <-howLong
	fmt.Printf("sleeping for %+v second(s)...\n", dur)
	<-time.After(time.Duration(dur) * time.Second)

	// remove howLong channel
	close(howLong)
}
