package packet

import "unicode/utf8"

const runeError = '\uFFFD'

func decodeRuneInString(s string) (rune, int) {
	return utf8.DecodeRuneInString(s)
}
