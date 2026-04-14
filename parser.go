package cron

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

// Parse returns a new crontab schedule representing the given spec.
// It panics with a descriptive error if the spec is not valid.
//
// It accepts
//   - Full crontab specs, e.g. "* * * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
func Parse(spec string) Schedule {
	schedule, err := ParseWithError(spec)
	if err != nil {
		log.Panic(err)
	}
	return schedule
}

// ParseWithError returns a new crontab schedule representing the given spec.
// Returns error if the spec is not valid.
//
// It accepts
//   - Full crontab specs, e.g. "* * * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
//   - Timezone prefix, e.g. "CRON_TZ=Asia/Shanghai 0 30 8 * * *"
//     or "TZ=America/New_York @daily"
func ParseWithError(spec string) (Schedule, error) {
	if len(spec) == 0 {
		return nil, errors.New("empty spec string")
	}

	// Extract CRON_TZ= or TZ= prefix
	var loc *time.Location
	if strings.HasPrefix(spec, "CRON_TZ=") || strings.HasPrefix(spec, "TZ=") {
		eqIdx := strings.IndexByte(spec, '=')
		rest := spec[eqIdx+1:]
		spIdx := strings.IndexByte(rest, ' ')
		if spIdx < 0 {
			return nil, fmt.Errorf("CRON_TZ/TZ= prefix requires a space before the cron expression: %s", spec)
		}
		tzName := rest[:spIdx]
		if len(tzName) == 0 {
			return nil, fmt.Errorf("empty timezone name in prefix: %s", spec)
		}
		var err error
		loc, err = time.LoadLocation(tzName)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone %q: %w", tzName, err)
		}
		spec = strings.TrimSpace(rest[spIdx+1:])
		if len(spec) == 0 {
			return nil, errors.New("empty spec string after timezone prefix")
		}
	}

	var schedule Schedule
	var err error
	if spec[0] == '@' {
		schedule, err = parseDescriptorWithError(spec)
	} else {
		schedule, err = parseSpecWithError(spec)
	}
	if err != nil {
		return nil, err
	}

	if loc != nil {
		return &TZSchedule{schedule: schedule, location: loc}, nil
	}
	return schedule, nil
}

// parseSpecWithError parses a standard cron spec (not a descriptor).
func parseSpecWithError(spec string) (Schedule, error) {

	// Zero-alloc whitespace field splitting using a stack array.
	var fields [6]string
	fieldCount := 0
	i := 0
	n := len(spec)
	for fieldCount < 6 && i < n {
		for i < n && (spec[i] == ' ' || spec[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}
		start := i
		for i < n && spec[i] != ' ' && spec[i] != '\t' {
			i++
		}
		fields[fieldCount] = spec[start:i]
		fieldCount++
	}
	// Check for extra fields beyond 6
	for i < n && (spec[i] == ' ' || spec[i] == '\t') {
		i++
	}
	if i < n {
		fieldCount = 7
	}

	if fieldCount != 5 && fieldCount != 6 {
		return nil, fmt.Errorf("expected 5 or 6 fields, found %d: %s", fieldCount, spec)
	}

	// If a sixth field is not provided (DayOfWeek), then it is equivalent to star.
	if fieldCount == 5 {
		fields[5] = "*"
	}

	var err error
	schedule := &SpecSchedule{}

	if schedule.Second, err = getFieldWithError(fields[0], seconds); err != nil {
		return nil, err
	}
	if schedule.Minute, err = getFieldWithError(fields[1], minutes); err != nil {
		return nil, err
	}
	if schedule.Hour, err = getFieldWithError(fields[2], hours); err != nil {
		return nil, err
	}
	if schedule.Dom, err = getFieldWithError(fields[3], dom); err != nil {
		return nil, err
	}
	if schedule.Month, err = getFieldWithError(fields[4], months); err != nil {
		return nil, err
	}
	if schedule.Dow, err = getFieldWithError(fields[5], dow); err != nil {
		return nil, err
	}

	return schedule, nil
}

// getField returns an Int with the bits set representing all of the times that
// the field represents.  A "field" is a comma-separated list of "ranges".
func getField(field string, r bounds) uint64 {
	bits, err := getFieldWithError(field, r)
	if err != nil {
		log.Panic(err)
	}
	return bits
}

// getFieldWithError returns an Int with the bits set representing all of the times that
// the field represents.  A "field" is a comma-separated list of "ranges".
func getFieldWithError(field string, r bounds) (uint64, error) {
	// Zero-alloc comma splitting using IndexByte.
	var bits uint64
	for len(field) > 0 {
		idx := strings.IndexByte(field, ',')
		var expr string
		if idx < 0 {
			expr = field
			field = ""
		} else {
			expr = field[:idx]
			field = field[idx+1:]
		}
		if len(expr) == 0 {
			continue
		}
		b, err := getRangeWithError(expr, r)
		if err != nil {
			return 0, err
		}
		bits |= b
	}
	return bits, nil
}

// getRange returns the bits indicated by the given expression:
//
//	number | number "-" number [ "/" number ]
func getRange(expr string, r bounds) uint64 {
	bits, err := getRangeWithError(expr, r)
	if err != nil {
		log.Panic(err)
	}
	return bits
}

// getRangeWithError returns the bits indicated by the given expression:
//
//	number | number "-" number [ "/" number ]
func getRangeWithError(expr string, r bounds) (uint64, error) {
	var start, end, step uint

	// Zero-alloc: split on "/" using IndexByte
	rangePart := expr
	stepPart := ""
	hasStep := false
	if slashIdx := strings.IndexByte(expr, '/'); slashIdx >= 0 {
		rangePart = expr[:slashIdx]
		rest := expr[slashIdx+1:]
		if strings.IndexByte(rest, '/') >= 0 {
			return 0, fmt.Errorf("too many slashes: %s", expr)
		}
		stepPart = rest
		hasStep = true
	}

	// Zero-alloc: split rangePart on "-" using IndexByte
	lowPart := rangePart
	highPart := ""
	hasHigh := false
	if hyphenIdx := strings.IndexByte(rangePart, '-'); hyphenIdx >= 0 {
		lowPart = rangePart[:hyphenIdx]
		rest := rangePart[hyphenIdx+1:]
		if strings.IndexByte(rest, '-') >= 0 {
			return 0, fmt.Errorf("too many hyphens: %s", expr)
		}
		highPart = rest
		hasHigh = true
	}

	singleDigit := !hasHigh

	var extraStar uint64
	if lowPart == "*" || lowPart == "?" {
		start = r.min
		end = r.max
		extraStar = starBit
	} else {
		var err error
		start, err = parseIntOrNameWithError(lowPart, r.names)
		if err != nil {
			return 0, err
		}
		if hasHigh {
			end, err = parseIntOrNameWithError(highPart, r.names)
			if err != nil {
				return 0, err
			}
		} else {
			end = start
		}
	}

	if !hasStep {
		step = 1
	} else {
		var err error
		step, err = mustParseIntWithError(stepPart)
		if err != nil {
			return 0, err
		}
		if step == 0 {
			return 0, fmt.Errorf("step must be > 0: %s", expr)
		}
		// Special handling: "N/step" means "N-max/step".
		if singleDigit {
			end = r.max
		}
	}

	if start < r.min {
		return 0, fmt.Errorf("beginning of range (%d) below minimum (%d): %s", start, r.min, expr)
	}
	if end > r.max {
		return 0, fmt.Errorf("end of range (%d) above maximum (%d): %s", end, r.max, expr)
	}
	if start > end {
		return 0, fmt.Errorf("beginning of range (%d) beyond end of range (%d): %s", start, end, expr)
	}

	return getBits(start, end, step) | extraStar, nil
}

// parseIntOrNameWithError returns the (possibly-named) integer contained in expr.
func parseIntOrNameWithError(expr string, names map[string]uint) (uint, error) {
	if names != nil {
		if namedInt, ok := names[strings.ToLower(expr)]; ok {
			return namedInt, nil
		}
	}
	return mustParseIntWithError(expr)
}

// mustParseIntWithError parses the given expression as an int or returns error.
func mustParseIntWithError(expr string) (uint, error) {
	num, err := strconv.Atoi(expr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int from %s: %s", expr, err)
	}
	if num < 0 {
		return 0, fmt.Errorf("negative number (%d) not allowed: %s", num, expr)
	}

	return uint(num), nil
}

// getBits sets all bits in the range [min, max], modulo the given step size.
func getBits(min, max, step uint) uint64 {
	var bits uint64

	// If step is 1, use shifts.
	if step == 1 {
		bits := max - min + 1
		if bits >= 64 {
			return math.MaxUint64
		}
		return (uint64(1)<<bits - 1) << min
	}

	// Else, use a simple loop.
	for i := min; i <= max; i += step {
		bits |= 1 << i
	}
	return bits
}

// all returns all bits within the given bounds.  (plus the star bit)
func all(r bounds) uint64 {
	return getBits(r.min, r.max, 1) | starBit
}

// parseDescriptorWithError returns a pre-defined schedule for the expression, or error
// if none matches.
func parseDescriptorWithError(spec string) (Schedule, error) {
	switch spec {
	case "@yearly", "@annually":
		return &SpecSchedule{
			Second: 1 << seconds.min,
			Minute: 1 << minutes.min,
			Hour:   1 << hours.min,
			Dom:    1 << dom.min,
			Month:  1 << months.min,
			Dow:    all(dow),
		}, nil

	case "@monthly":
		return &SpecSchedule{
			Second: 1 << seconds.min,
			Minute: 1 << minutes.min,
			Hour:   1 << hours.min,
			Dom:    1 << dom.min,
			Month:  all(months),
			Dow:    all(dow),
		}, nil

	case "@weekly":
		return &SpecSchedule{
			Second: 1 << seconds.min,
			Minute: 1 << minutes.min,
			Hour:   1 << hours.min,
			Dom:    all(dom),
			Month:  all(months),
			Dow:    1 << dow.min,
		}, nil

	case "@daily", "@midnight":
		return &SpecSchedule{
			Second: 1 << seconds.min,
			Minute: 1 << minutes.min,
			Hour:   1 << hours.min,
			Dom:    all(dom),
			Month:  all(months),
			Dow:    all(dow),
		}, nil

	case "@hourly":
		return &SpecSchedule{
			Second: 1 << seconds.min,
			Minute: 1 << minutes.min,
			Hour:   all(hours),
			Dom:    all(dom),
			Month:  all(months),
			Dow:    all(dow),
		}, nil

	case "@reboot":
		return &RebootSchedule{}, nil
	}

	const every = "@every "
	if strings.HasPrefix(spec, every) {
		duration, err := time.ParseDuration(spec[len(every):])
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration %s: %s", spec, err)
		}
		if duration < time.Second {
			return nil, fmt.Errorf("cron/constantdelay: delays of less than a second are not supported: %s", duration.String())
		}
		return Every(duration), nil
	}

	return nil, fmt.Errorf("unrecognized descriptor: %s", spec)
}
