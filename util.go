package dejavu

import (
	"crypto/sha1"
	"fmt"
)

func Hash(data []byte) string {
	return fmt.Sprintf("%x", sha1.Sum(data))
}
