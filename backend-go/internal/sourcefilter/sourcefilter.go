package sourcefilter

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const RuleVersion = "research-routing-v1"

type Config struct {
	Enabled   bool
	Whitelist []string
	Blacklist []string
}

type Decision struct {
	Profile          string
	Blocked          bool
	MatchedWhitelist []string
	MatchedBlacklist []string
}

type ValidationIssue struct {
	Field   string `json:"field"`
	Index   int    `json:"index"`
	Value   string `json:"value"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type NormalizationWarning struct {
	Field   string `json:"field"`
	Value   string `json:"value"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NormalizeLists(whitelist, blacklist []string) ([]string, []string, []ValidationIssue, []NormalizationWarning) {
	white, whiteIssues, whiteWarnings := normalizeList("whitelist_keywords", whitelist)
	black, blackIssues, blackWarnings := normalizeList("blacklist_keywords", blacklist)
	issues := append(whiteIssues, blackIssues...)
	warnings := append(whiteWarnings, blackWarnings...)

	blackKeys := make(map[string]int, len(black))
	for index, value := range black {
		blackKeys[matchText(value)] = index
	}
	for index, value := range white {
		if blackIndex, exists := blackKeys[matchText(value)]; exists {
			issues = append(issues, ValidationIssue{
				Field: "whitelist_keywords", Index: index, Value: value, Code: "cross_list_conflict",
				Message: "同一关键字不能同时出现在白名单和黑名单中",
			})
			issues = append(issues, ValidationIssue{
				Field: "blacklist_keywords", Index: blackIndex, Value: value, Code: "cross_list_conflict",
				Message: "同一关键字不能同时出现在白名单和黑名单中",
			})
		}
	}
	return white, black, issues, warnings
}

func normalizeList(field string, values []string) ([]string, []ValidationIssue, []NormalizationWarning) {
	result := make([]string, 0, len(values))
	issues := make([]ValidationIssue, 0)
	warnings := make([]NormalizationWarning, 0)
	seen := map[string]struct{}{}
	for index, raw := range values {
		value := normalizeValue(raw)
		if value == "" {
			continue
		}
		if strings.ContainsAny(raw, ",，\r\n") {
			issues = append(issues, ValidationIssue{Field: field, Index: index, Value: raw, Code: "embedded_separator", Message: "单个关键字不能包含逗号或换行"})
			continue
		}
		if containsControl(value) || !utf8.ValidString(value) {
			issues = append(issues, ValidationIssue{Field: field, Index: index, Value: raw, Code: "invalid_character", Message: "关键字包含不可用的控制字符"})
			continue
		}
		if len([]rune(value)) > 80 {
			issues = append(issues, ValidationIssue{Field: field, Index: index, Value: raw, Code: "too_long", Message: "关键字不能超过 80 个字符"})
			continue
		}
		key := matchText(value)
		if _, exists := seen[key]; exists {
			warnings = append(warnings, NormalizationWarning{Field: field, Value: value, Code: "duplicate_removed", Message: "重复关键字已自动移除"})
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	if len(result) > 200 {
		issues = append(issues, ValidationIssue{Field: field, Index: -1, Code: "too_many", Message: "每组关键字不能超过 200 个"})
	}
	return result, issues, warnings
}

func Evaluate(title string, cfg Config) Decision {
	if !cfg.Enabled {
		return Decision{Profile: "fast"}
	}
	black := matchedKeywords(title, cfg.Blacklist)
	white := matchedKeywords(title, cfg.Whitelist)
	if len(black) > 0 {
		return Decision{Profile: "blocked", Blocked: true, MatchedWhitelist: white, MatchedBlacklist: black}
	}
	if len(white) > 0 {
		return Decision{Profile: "deep", MatchedWhitelist: white}
	}
	return Decision{Profile: "fast"}
}

func matchedKeywords(title string, keywords []string) []string {
	candidate := matchText(title)
	result := make([]string, 0)
	for _, keyword := range keywords {
		normalized := matchText(keyword)
		if normalized != "" && keywordMatches(candidate, normalized) {
			result = append(result, keyword)
		}
	}
	return result
}

func keywordMatches(candidate, keyword string) bool {
	if !isShortASCIIToken(keyword) {
		return strings.Contains(candidate, keyword)
	}
	for start := 0; start <= len(candidate)-len(keyword); {
		offset := strings.Index(candidate[start:], keyword)
		if offset < 0 {
			return false
		}
		left := start + offset
		right := left + len(keyword)
		if (left == 0 || !isASCIIAlphaNumeric(candidate[left-1])) && (right == len(candidate) || !isASCIIAlphaNumeric(candidate[right])) {
			return true
		}
		start = left + 1
	}
	return false
}

func isShortASCIIToken(value string) bool {
	if len(value) == 0 || len(value) > 5 {
		return false
	}
	for index := 0; index < len(value); index++ {
		char := value[index]
		if !isASCIIAlphaNumeric(char) && char != '.' {
			return false
		}
	}
	return true
}

func isASCIIAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func normalizeValue(value string) string {
	return strings.Join(strings.Fields(norm.NFKC.String(strings.TrimSpace(value))), " ")
}

func matchText(value string) string {
	return strings.ToLower(normalizeValue(value))
}

func containsControl(value string) bool {
	for _, current := range value {
		if unicode.IsControl(current) {
			return true
		}
	}
	return false
}
