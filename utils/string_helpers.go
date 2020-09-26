package utils

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

const (
	// minport is the minimum port number
	minport = 10

	// maxport is the maximum port number
	maxport = 20000
)

// NormalizeString removes leading and trailing characters that are
// neither numbers nor alphabetic letter. The resulting string is
// casted to lowercase and returned
func NormalizeString(s string) string {
	keepCharFunc := func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	}

	s = strings.TrimLeftFunc(s, keepCharFunc)
	s = strings.TrimRightFunc(s, keepCharFunc)
	return strings.ToLower(s)
}

// GetPort extracts the port number from an address string
func GetPort(addr string) (string, error) {
	tks := strings.Split(addr, ":")
	if len(tks) != 2 {
		return "", errors.New("getport: invalid address")
	}

	if port, err := strconv.Atoi(tks[1]); err != nil || port < minport ||
		port > maxport {
		return "", errors.New("getport: invalid port")
	}
	return tks[1], nil
}
