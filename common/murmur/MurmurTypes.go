package murmur


const (
	// a prime number that serves as a multiplier during mixing. Distributes bits and improves randomness
	c32_1 = 0x85ebca6b
	// a prime number also used for mixing. Enhances distribution of hash value
	c32_2 = 0xc2b2ae35
	// added to hash after each chunk is mixed in. Contributes to finalization step
	c32_3 = 0xe6546b64
	// multiplied in the finalization step. Provides additional mixing effect
	c32_4 = 0x1b873593
	// multiplier in the finalization step. Again, improves hash value distribution
	c32_5 = 0x5c4bcea9
)