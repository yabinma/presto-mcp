package config

import "testing"

func BenchmarkParse(b *testing.B) {
	data := []byte(validLocal)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(data); err != nil {
			b.Fatal(err)
		}
	}
}
