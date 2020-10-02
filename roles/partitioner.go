package roles

// Partition computes the partition a key should be assigned to
func Partition(key string, parts int) int {
	var c int = int(key[0]) - int('a')
	if c < 0 {
		return 0
	}
	if c >= 26 {
		return parts - 1
	}
	return c % parts
}
