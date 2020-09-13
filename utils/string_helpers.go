package utils

import (
	"strings"
	"unicode"
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
