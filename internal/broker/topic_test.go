package broker_test

import (
	"testing"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
)

func TestTopicMatching(t *testing.T) {
	business := "通配符订阅"
	cases := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		{"精确匹配", "a/b/c", "a/b/c", true},
		{"精确不匹配", "a/b", "a/b/c", false},
		{"单层加号命中", "a/+/c", "a/b/c", true},
		{"单层加号跨层不命中", "a/+", "a/b/c", false},
		{"多层井号命中", "a/#", "a/b/c", true},
		{"井号命中本级", "a/#", "a", true},
		{"井号不匹配零层级", "#", "", false},
		{"根井号命中", "#", "anything/here", true},
		{"金融行情层级", "finance/+/stock/#", "finance/nasdaq/stock/AAPL/tick", true},
		{"前缀不匹配", "sport/tennis/#", "sport/basketball/x", false},
		{"加号不匹配多级", "+/+", "a/b/c", false},
		{"空层级命中", "a//c", "a//c", true},
		{"加号匹配空层级", "a/+/c", "a//c", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mqtt3.Match(tc.filter, tc.topic)
			result := "PASS"
			if got != tc.want {
				result = "FAIL"
			}
			caseLog(business, tc.name, result, "")
			if got != tc.want {
				t.Fatalf("Match(%q,%q)=%v want %v", tc.filter, tc.topic, got, tc.want)
			}
		})
	}
}

func TestTopicValidation(t *testing.T) {
	business := "主题合法性"
	cases := []struct {
		name   string
		filter string
		valid  bool
	}{
		{"普通过滤器", "a/b/c", true},
		{"加号整层", "a/+/c", true},
		{"井号结尾", "a/#", true},
		{"单独井号", "#", true},
		{"井号不在末层", "a/#/c", false},
		{"加号粘连字符", "a/b+/c", false},
		{"井号粘连字符", "a/b#", false},
		{"空过滤器", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mqtt3.ValidateFilter(tc.filter)
			ok := err == nil
			result := "PASS"
			if ok != tc.valid {
				result = "FAIL"
			}
			caseLog(business, tc.name, result, "")
			if ok != tc.valid {
				t.Fatalf("ValidateFilter(%q)=%v want valid=%v", tc.filter, err, tc.valid)
			}
		})
	}

	pubCases := []struct {
		name  string
		topic string
		valid bool
	}{
		{"普通主题", "dev/1/temp", true},
		{"空主题", "", false},
		{"发布主题含加号", "a/+/c", false},
		{"发布主题含井号", "a/#", false},
	}
	for _, tc := range pubCases {
		t.Run("发布_"+tc.name, func(t *testing.T) {
			err := mqtt3.ValidatePublishTopic(tc.topic)
			ok := err == nil
			result := "PASS"
			if ok != tc.valid {
				result = "FAIL"
			}
			caseLog(business, "发布_"+tc.name, result, "")
			if ok != tc.valid {
				t.Fatalf("ValidatePublishTopic(%q)=%v want %v", tc.topic, err, tc.valid)
			}
		})
	}
}
