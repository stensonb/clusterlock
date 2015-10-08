package sleepManager

import "time"

type Event int
type EventChannel chan Event
type SleepDurationChannel chan uint
type SleepDurationRequestChannel chan SleepDurationChannel

const (
	SUCCESS Event = iota
	ERROR
)

const SLEEP_DURATION_MAX = uint(60)
const SLEEP_DURATION_INITIAL = uint(1)

type SleepManager struct {
	EventChannel  EventChannel
	sleepDuration uint
	sleepNow      SleepDurationRequestChannel // reports how long to sleep now
}

func NewSleepManager() *SleepManager {
	ans := new(SleepManager)
	ans.EventChannel = make(EventChannel)
	ans.sleepDuration = SLEEP_DURATION_INITIAL
	ans.sleepNow = make(SleepDurationRequestChannel)

	go func() {
		for {
			select {
			case event := <-ans.EventChannel:
				switch event {
				case SUCCESS:
					ans.sleepDuration = SLEEP_DURATION_INITIAL
				case ERROR:
					ans.sleepDuration = ans.sleepDuration << 1 // sleep twice as long
					if ans.sleepDuration > SLEEP_DURATION_MAX {
						ans.sleepDuration = SLEEP_DURATION_MAX
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
	<-time.After(time.Duration(<-howLong) * time.Second)

	// remove howLong channel
	close(howLong)
}
