package deleter

import (
	"testing"
	"time"
)

func TestThroughputFromSamples_Empty(t *testing.T) {
	if got := ThroughputFromSamples(nil); got != 0 {
		t.Errorf("nil samples: got %v want 0", got)
	}
	if got := ThroughputFromSamples([]throughputSample{{T: time.Now(), Deleted: 5}}); got != 0 {
		t.Errorf("single sample: got %v want 0", got)
	}
}

func TestThroughputFromSamples_Basic(t *testing.T) {
	t0 := time.Now()
	samples := []throughputSample{
		NewSample(t0, 0),
		NewSample(t0.Add(2*time.Second), 200),
	}
	got := ThroughputFromSamples(samples)
	if got < 99 || got > 101 {
		t.Errorf("expected ~100 obj/s, got %v", got)
	}
}

func TestThroughputFromSamples_ZeroDuration(t *testing.T) {
	t0 := time.Now()
	samples := []throughputSample{
		NewSample(t0, 0),
		NewSample(t0, 100),
	}
	if got := ThroughputFromSamples(samples); got != 0 {
		t.Errorf("zero-duration window: got %v want 0", got)
	}
}
