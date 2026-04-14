package cron

import (
	"reflect"
	"testing"
	"time"
	_ "time/tzdata" // Embed timezone database for CI environments without system tzdata
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

func TestParseCronTZ(t *testing.T) {
	tests := []struct {
		expr        string
		shouldError bool
		isTZ        bool
	}{
		// Valid CRON_TZ= prefix
		{"CRON_TZ=Asia/Shanghai 0 30 8 * * *", false, true},
		{"TZ=America/New_York 0 0 9 * * *", false, true},
		{"TZ=UTC @daily", false, true},
		{"CRON_TZ=Europe/London @hourly", false, true},
		{"CRON_TZ=Asia/Tokyo @every 30s", false, true},

		// No prefix — should work as before, not TZSchedule
		{"0 30 8 * * *", false, false},
		{"@daily", false, false},

		// Invalid timezone
		{"CRON_TZ=Invalid/Zone 0 0 0 * * *", true, false},
		{"TZ=Not_A_Zone @daily", true, false},

		// Missing space after timezone
		{"CRON_TZ=Asia/Shanghai", true, false},

		// Empty spec after timezone
		{"CRON_TZ=UTC ", true, false},

		// Empty timezone name
		{"TZ= @daily", true, false},
		{"CRON_TZ= 0 0 0 * * *", true, false},
	}

	for _, c := range tests {
		schedule, err := ParseWithError(c.expr)
		if c.shouldError {
			if err == nil {
				t.Errorf("%s => expected error but got none", c.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s => unexpected error: %v", c.expr, err)
			continue
		}
		_, isTZ := schedule.(*TZSchedule)
		if isTZ != c.isTZ {
			t.Errorf("%s => TZSchedule=%v, want %v", c.expr, isTZ, c.isTZ)
		}
	}
}

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return loc
}

func TestTZScheduleNext(t *testing.T) {
	shanghai := mustLoadLocation(t, "Asia/Shanghai")
	ny := mustLoadLocation(t, "America/New_York")
	tokyo := mustLoadLocation(t, "Asia/Tokyo")
	london := mustLoadLocation(t, "Europe/London")

	tests := []struct {
		name     string
		spec     string
		fromTime time.Time
		expected time.Time
	}{
		// === Basic timezone offset ===
		{
			name:     "Shanghai daily 08:30",
			spec:     "CRON_TZ=Asia/Shanghai 0 30 8 * * *",
			fromTime: time.Date(2026, 4, 14, 0, 0, 0, 0, time.UTC),
			expected: time.Date(2026, 4, 14, 8, 30, 0, 0, shanghai), // 00:30 UTC
		},
		{
			name:     "New York daily 09:00 (EDT, UTC-4)",
			spec:     "TZ=America/New_York 0 0 9 * * *",
			fromTime: time.Date(2026, 4, 14, 0, 0, 0, 0, time.UTC),
			expected: time.Date(2026, 4, 14, 9, 0, 0, 0, ny), // 13:00 UTC
		},
		{
			name:     "UTC daily midnight, from just past midnight",
			spec:     "CRON_TZ=UTC 0 0 0 * * *",
			fromTime: time.Date(2026, 4, 14, 0, 0, 1, 0, time.UTC),
			expected: time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC),
		},

		// === Cross-day due to timezone offset ===
		{
			name:     "Shanghai 01:00 is previous day 17:00 UTC",
			spec:     "CRON_TZ=Asia/Shanghai 0 0 1 * * *",
			fromTime: time.Date(2026, 4, 14, 16, 0, 0, 0, time.UTC), // Apr 15 00:00 Shanghai
			expected: time.Date(2026, 4, 15, 1, 0, 0, 0, shanghai),   // Apr 14 17:00 UTC
		},
		{
			name:     "NY 23:00 is next day 03:00 UTC (EDT)",
			spec:     "CRON_TZ=America/New_York 0 0 23 * * *",
			fromTime: time.Date(2026, 4, 14, 0, 0, 0, 0, ny),
			expected: time.Date(2026, 4, 14, 23, 0, 0, 0, ny), // Apr 15 03:00 UTC
		},

		// === Same expression, different timezone = different UTC ===
		{
			name:     "09:00 in Tokyo from midnight Tokyo — fires same day",
			spec:     "CRON_TZ=Asia/Tokyo 0 0 9 * * *",
			fromTime: time.Date(2026, 4, 14, 0, 0, 0, 0, tokyo), // midnight Tokyo, 09:00 is ahead
			expected: time.Date(2026, 4, 14, 9, 0, 0, 0, tokyo),  // same day 09:00 Tokyo = 00:00 UTC
		},
		{
			name:     "09:00 in Tokyo from after 09:00 — fires next day",
			spec:     "CRON_TZ=Asia/Tokyo 0 0 9 * * *",
			fromTime: time.Date(2026, 4, 14, 10, 0, 0, 0, tokyo), // 10:00 Tokyo, past 09:00
			expected: time.Date(2026, 4, 15, 9, 0, 0, 0, tokyo),   // next day
		},

		// === DST spring forward: March 8, 2026 NY clocks jump 2:00→3:00 ===
		{
			name:     "DST spring forward: 2:30 AM doesn't exist, skip to next day",
			spec:     "CRON_TZ=America/New_York 0 30 2 * * *",
			fromTime: time.Date(2026, 3, 8, 1, 0, 0, 0, ny),
			// 2:30 AM doesn't exist on March 8 (clocks skip from 2:00 to 3:00).
			// SpecSchedule.Next can't find a match on the 8th, so it advances
			// to March 9 when 2:30 AM exists again (now in EDT).
			expected: time.Date(2026, 3, 9, 2, 30, 0, 0, ny),
		},
		{
			name:     "DST spring forward: 3:00 AM exists normally",
			spec:     "CRON_TZ=America/New_York 0 0 3 * * *",
			fromTime: time.Date(2026, 3, 8, 1, 0, 0, 0, ny),
			expected: time.Date(2026, 3, 8, 3, 0, 0, 0, ny),
		},

		// === DST fall back: Nov 1, 2026 NY clocks go 2:00→1:00 ===
		{
			name:     "DST fall back: 1:30 AM runs in first occurrence (EDT)",
			spec:     "CRON_TZ=America/New_York 0 30 1 * * *",
			fromTime: time.Date(2026, 11, 1, 0, 0, 0, 0, ny),
			expected: time.Date(2026, 11, 1, 1, 30, 0, 0, ny),
		},

		// === Day-of-week affected by timezone ===
		{
			name:     "Monday in Shanghai but still Sunday in UTC",
			spec:     "CRON_TZ=Asia/Shanghai 0 0 1 * * 1", // Monday 01:00 Shanghai
			fromTime: time.Date(2026, 4, 12, 16, 0, 0, 0, time.UTC), // Sun Apr 12 16:00 UTC = Mon Apr 13 00:00 Shanghai
			expected: time.Date(2026, 4, 13, 1, 0, 0, 0, shanghai),   // Mon 01:00 Shanghai = Sun 17:00 UTC
		},

		// === Cross month/year boundary ===
		{
			name:     "Cross month: Shanghai Dec 31 23:30 → Jan 1 next year",
			spec:     "CRON_TZ=Asia/Shanghai 0 0 0 1 * *", // 1st of every month at midnight
			fromTime: time.Date(2026, 12, 31, 23, 30, 0, 0, shanghai),
			expected: time.Date(2027, 1, 1, 0, 0, 0, 0, shanghai),
		},

		// === London BST (UTC+1 in summer) ===
		{
			name:     "London summer time (BST, UTC+1)",
			spec:     "CRON_TZ=Europe/London 0 0 9 * * *",
			fromTime: time.Date(2026, 7, 1, 7, 0, 0, 0, time.UTC), // 08:00 BST
			expected: time.Date(2026, 7, 1, 9, 0, 0, 0, london),    // 08:00 UTC
		},

		// === Descriptor with timezone ===
		{
			name:     "@daily with Tokyo timezone",
			spec:     "CRON_TZ=Asia/Tokyo @daily",
			fromTime: time.Date(2026, 4, 14, 14, 0, 0, 0, time.UTC), // Apr 14 23:00 Tokyo
			expected: time.Date(2026, 4, 15, 0, 0, 0, 0, tokyo),     // Apr 14 15:00 UTC
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schedule := Parse(tc.spec)
			actual := schedule.Next(tc.fromTime)
			if !actual.UTC().Equal(tc.expected.UTC()) {
				t.Errorf("Next(%v) = %v (UTC: %v), want %v (UTC: %v)",
					tc.fromTime, actual, actual.UTC(), tc.expected, tc.expected.UTC())
			}
		})
	}
}

func TestTZScheduleConsecutiveNext(t *testing.T) {
	// Verify consecutive Next() calls produce correct sequence
	shanghai := mustLoadLocation(t, "Asia/Shanghai")
	schedule := Parse("CRON_TZ=Asia/Shanghai 0 0 9 * * *") // daily 09:00 Shanghai

	current := time.Date(2026, 4, 14, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		next := schedule.Next(current)
		expectedDay := 14 + i
		expected := time.Date(2026, 4, expectedDay, 9, 0, 0, 0, shanghai)
		if !next.UTC().Equal(expected.UTC()) {
			t.Fatalf("iteration %d: Next(%v) = %v (UTC: %v), want %v (UTC: %v)",
				i, current, next, next.UTC(), expected, expected.UTC())
		}
		current = next
	}
}

func TestTZScheduleEveryIsUnaffected(t *testing.T) {
	// @every is a constant delay — timezone wrapping should not change the interval
	spec := "CRON_TZ=Asia/Tokyo @every 30s"
	schedule := Parse(spec)

	from := time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)
	next := schedule.Next(from)
	expected := from.Add(30 * time.Second)
	if !next.UTC().Equal(expected.UTC()) {
		t.Errorf("@every with TZ: Next(%v) = %v, want %v", from, next.UTC(), expected.UTC())
	}
}

func TestTZScheduleSameSpecDifferentTimezones(t *testing.T) {
	// Same "09:00 daily" in different timezones must fire at different UTC times
	shanghaiSched := Parse("CRON_TZ=Asia/Shanghai 0 0 9 * * *")
	nySched := Parse("CRON_TZ=America/New_York 0 0 9 * * *")

	from := time.Date(2026, 4, 14, 0, 0, 0, 0, time.UTC)
	shanghaiNext := shanghaiSched.Next(from).UTC()
	nyNext := nySched.Next(from).UTC()

	// Shanghai 09:00 = UTC 01:00, NY 09:00 EDT = UTC 13:00 — 12 hour gap
	diff := nyNext.Sub(shanghaiNext)
	if diff != 12*time.Hour {
		t.Errorf("Shanghai fires at %v UTC, NY fires at %v UTC, diff=%v, want 12h",
			shanghaiNext, nyNext, diff)
	}
}
