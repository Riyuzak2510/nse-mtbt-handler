package sharding

import "testing"

func TestPinnedTokenRouting(t *testing.T) {
	for _, count := range []int{1, 2, 3, 16} {
		for _, token := range []int32{1, 2, 3, 900001, 2147483647} {
			shard, err := ForToken(token, count)
			if err != nil || shard != int(token%int32(count)) || shard < 0 || shard >= count {
				t.Fatalf("%d/%d: %d,%v", token, count, shard, err)
			}
		}
	}
	for _, count := range []int{-1, 0, 17, 2147483647} {
		if _, err := ForToken(1, count); err == nil {
			t.Fatalf("accepted invalid count %d", count)
		}
	}
	for _, token := range []int32{-1, 0} {
		if _, err := ForToken(token, 3); err == nil {
			t.Fatalf("accepted invalid token %d", token)
		}
	}
}
