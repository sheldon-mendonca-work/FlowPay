package utils

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

func ComputeHash(payload []byte) (string, error) {
	h := xxhash.New()
	h.Write(payload)                         // Directly write the bytes
	return fmt.Sprintf("%x", h.Sum64()), nil // %x gives you a nice Hex string
}
