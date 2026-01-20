package cron

import (
	"reflect"
	"testing"
	"time"
)

func TestRange(t *testing.T) {
	ranges := []struct {
		expr     string
		min, max uint
		expected uint64
	}{
		{"5", 0, 7, 1 << 5},
		{"0", 0, 7, 1 << 0},
		{"7", 0, 7, 1 << 7},

		{"5-5", 0, 7, 1 << 5},
		{"5-6", 0, 7, 1<<5 | 1<<6},
		{"5-7", 0, 7, 1<<5 | 1<<6 | 1<<7},

		{"5-6/2", 0, 7, 1 << 5},
		{"5-7/2", 0, 7, 1<<5 | 1<<7},
		{"5-7/1", 0, 7, 1<<5 | 1<<6 | 1<<7},

		{"*", 1, 3, 1<<1 | 1<<2 | 1<<3 | starBit},
		{"*/2", 1, 3, 1<<1 | 1<<3 | starBit},
	}

	for _, c := range ranges {
		actual := getRange(c.expr, bounds{c.min, c.max, nil})
		if actual != c.expected {
			t.Errorf("%s => (expected) %d != %d (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestField(t *testing.T) {
	fields := []struct {
		expr     string
		min, max uint
		expected uint64
	}{
		{"5", 1, 7, 1 << 5},
		{"5,6", 1, 7, 1<<5 | 1<<6},
		{"5,6,7", 1, 7, 1<<5 | 1<<6 | 1<<7},
		{"1,5-7/2,3", 1, 7, 1<<1 | 1<<5 | 1<<7 | 1<<3},
	}

	for _, c := range fields {
		actual := getField(c.expr, bounds{c.min, c.max, nil})
		if actual != c.expected {
			t.Errorf("%s => (expected) %d != %d (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestBits(t *testing.T) {
	allBits := []struct {
		r        bounds
		expected uint64
	}{
		{minutes, 0xfffffffffffffff}, // 0-59: 60 ones
		{hours, 0xffffff},            // 0-23: 24 ones
		{dom, 0xfffffffe},            // 1-31: 31 ones, 1 zero
		{months, 0x1ffe},             // 1-12: 12 ones, 1 zero
		{dow, 0x7f},                  // 0-6: 7 ones
	}

	for _, c := range allBits {
		actual := all(c.r) // all() adds the starBit, so compensate for that..
		if c.expected|starBit != actual {
			t.Errorf("%d-%d/%d => (expected) %b != %b (actual)",
				c.r.min, c.r.max, 1, c.expected|starBit, actual)
		}
	}

	bits := []struct {
		min, max, step uint
		expected       uint64
	}{

		{0, 0, 1, 0x1},
		{1, 1, 1, 0x2},
		{1, 5, 2, 0x2a}, // 101010
		{1, 4, 2, 0xa},  // 1010
	}

	for _, c := range bits {
		actual := getBits(c.min, c.max, c.step)
		if c.expected != actual {
			t.Errorf("%d-%d/%d => (expected) %b != %b (actual)",
				c.min, c.max, c.step, c.expected, actual)
		}
	}
}

func TestSpecSchedule(t *testing.T) {
	entries := []struct {
		expr     string
		expected Schedule
	}{
		{"* 5 * * * *", &SpecSchedule{all(seconds), 1 << 5, all(hours), all(dom), all(months), all(dow)}},
		{"@every 5m", ConstantDelaySchedule{time.Duration(5) * time.Minute}},
		{"@reboot", &RebootSchedule{}},
	}

	for _, c := range entries {
		actual := Parse(c.expr)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s => (expected) %b != %b (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestParseDescriptors(t *testing.T) {
	tests := []struct {
		expr     string
		expected Schedule
	}{
		{"@yearly", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, 1 << 1, 1 << 1, all(dow)}},
		{"@annually", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, 1 << 1, 1 << 1, all(dow)}},
		{"@monthly", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, 1 << 1, all(months), all(dow)}},
		{"@weekly", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), 1 << 0}},
		{"@daily", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), all(dow)}},
		{"@midnight", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), all(dow)}},
		{"@hourly", &SpecSchedule{1 << 0, 1 << 0, all(hours), all(dom), all(months), all(dow)}},
		{"@every 1h", ConstantDelaySchedule{time.Hour}},
		{"@every 30s", ConstantDelaySchedule{30 * time.Second}},
	}

	for _, c := range tests {
		actual := Parse(c.expr)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s => (expected) %v != %v (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestParseWithNames(t *testing.T) {
	tests := []struct {
		expr     string
		expected Schedule
	}{
		{"0 0 0 * JAN *", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), 1 << 1, all(dow)}},
		{"0 0 0 * * MON", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), 1 << 1}},
		{"0 0 0 * JAN-MAR *", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), 1<<1 | 1<<2 | 1<<3, all(dow)}},
		{"0 0 0 * * MON-FRI", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), 1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5}},
	}

	for _, c := range tests {
		actual := Parse(c.expr)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s => (expected) %v != %v (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestParseWithError(t *testing.T) {
	tests := []struct {
		expr        string
		shouldError bool
	}{
		{"", true},
		{"* * * *", true},
		{"* * * * * * *", true},
		{"60 * * * * *", true},
		{"* 60 * * * *", true},
		{"* * 24 * * *", true},
		{"* * * 32 * *", true},
		{"* * * * 13 *", true},
		{"* * * * * 7", true},
		{"* * * 0 * *", true},
		{"* * * * 0 *", true},
		{"5-3 * * * * *", true},
		{"@invalid", true},
		{"@every invalid", true},
		{"* * * * * *", false},
		{"0 0 0 1 1 *", false},
		{"59 59 23 31 12 6", false},
	}

	for _, c := range tests {
		_, err := ParseWithError(c.expr)
		if c.shouldError && err == nil {
			t.Errorf("%s => expected error but got none", c.expr)
		}
		if !c.shouldError && err != nil {
			t.Errorf("%s => unexpected error: %v", c.expr, err)
		}
	}
}

func TestQuestionMark(t *testing.T) {
	tests := []struct {
		expr     string
		expected Schedule
	}{
		{"? ? ? ? ? ?", &SpecSchedule{all(seconds), all(minutes), all(hours), all(dom), all(months), all(dow)}},
		{"0 0 0 ? * *", &SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), all(dow)}},
	}

	for _, c := range tests {
		actual := Parse(c.expr)
		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("%s => (expected) %v != %v (actual)", c.expr, c.expected, actual)
		}
	}
}

func TestComplexExpressions(t *testing.T) {
	tests := []struct {
		expr string
	}{
		{"0 0 12 * * ?"},
		{"0 15 10 * * ?"},
		{"0 0/5 14 * * ?"},
		{"0 0-5 14 * * ?"},
		{"0 10,44 14 * 3 3"},
		{"0 15 10 * * 1-5"},
		{"*/5 * * * * *"},
		{"0 */5 * * * *"},
	}

	for _, c := range tests {
		_, err := ParseWithError(c.expr)
		if err != nil {
			t.Errorf("%s => unexpected error: %v", c.expr, err)
		}
	}
}
