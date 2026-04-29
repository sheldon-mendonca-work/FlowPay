package utils

import "strings"

func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	isErrorPresent := strings.Contains(err.Error(), "duplicate key value")
	return isErrorPresent
}
