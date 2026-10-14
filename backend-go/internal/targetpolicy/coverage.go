package targetpolicy

import (
	"regexp"
	"strings"
)

// The vocabulary is applied as a full-coverage rule. Every rune of a label has to be
// explained by a macro/industry term or by a neutral qualifier (a region, a scope word,
// a measure word). A label that leaves a residue behind - a proper name, a verb, a
// quantity with a unit - names an actor or a news clause rather than an economic
// object, and is rejected even when it contains a genuine keyword. Plain substring
// matching kept public targets such as "迪拜航空的副驾驶" (航空) and "马来亚银行" (银行).

// numericTenorPattern accepts the tenor/period spellings that qualify an indicator
// ("10年期国债收益率") but never a quantity ("2800万桶", "30064辆").
var numericTenorPattern = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?(?:年期|年度|个月|季度|周|天)$`)

const (
	maxCoverSegments = 6
	// maxCoverWordRunes matches maxPlausibleNameRunes so a long vocabulary word such as
	// "semiconductor" is still reachable as a single word.
	maxCoverWordRunes = 24
)

// lexiconWord records what a vocabulary word contributes to a label. A qualifier has
// an empty kind and can never make a label publishable on its own.
type lexiconWord struct{ kind string }

var targetLexicon = buildTargetLexicon()

// genericFlowWords name a quantity, a price or an action without naming the object it
// belongs to. A label whose only terms are generic ("库存", "出口") is too vague to
// publish as a target; "铜库存" stays valid because 铜 is a concrete object.
var genericFlowWords = genericFlowWordSet()

func genericFlowWordSet() map[string]bool {
	words := []string{
		"库存", "供应", "供给", "产量", "出口", "进口", "储备", "产能", "需求", "价格",
		"贸易", "运量", "开工率", "停产", "检修", "投放", "拍卖", "招标", "中标", "配额",
		"许可证", "禁令", "限制", "增持", "减持", "释放", "补贴",
	}
	result := make(map[string]bool, len(words))
	for _, word := range words {
		result[Normalized(word)] = true
	}
	return result
}

func buildTargetLexicon() map[string]lexiconWord {
	lexicon := make(map[string]lexiconWord, 1024)
	put := func(word, kind string) {
		key := Normalized(word)
		if key == "" {
			return
		}
		if _, exists := lexicon[key]; !exists {
			lexicon[key] = lexiconWord{kind: kind}
		}
	}
	// Macro terms are registered first so a policy-rate or indicator word keeps its
	// specific kind instead of collapsing into the industry kind of an alias.
	for _, check := range macroKindChecks {
		for _, token := range check.tokens {
			put(token, check.kind)
		}
	}
	for _, entry := range industryEntries {
		put(entry.label, Sector)
		for _, alias := range entry.aliases {
			put(alias, Sector)
		}
	}
	for _, word := range qualifierWords {
		put(word, "")
	}
	return lexicon
}

// kindPriority mirrors the order of macroKindChecks, so a flow or rate object outranks
// an industry or commodity word inside the same label.
var kindPriority = buildKindPriority()

func buildKindPriority() map[string]int {
	priority := map[string]int{}
	for index, check := range macroKindChecks {
		if _, exists := priority[check.kind]; !exists {
			priority[check.kind] = index
		}
	}
	return priority
}

// coveredKinds returns the target kinds a compact label is built from. ok=false means
// the label is not fully explained by the vocabulary, or it never names a concrete
// object (only generic flow words such as "库存").
func coveredKinds(compact string) ([]string, bool) {
	if parts := strings.FieldsFunc(compact, func(current rune) bool {
		return current == '/' || current == '-' || current == '&' || current == ':'
	}); len(parts) > 1 {
		kinds := make([]string, 0, len(parts))
		for _, part := range parts {
			partKinds, ok := coveredKinds(part)
			if !ok {
				return nil, false
			}
			kinds = append(kinds, partKinds...)
		}
		return kinds, true
	}
	return segmentKinds(compact)
}

// segmentKinds finds the shortest word segmentation of the label. Words may only come
// from the lexicon, so any unknown residue makes the label unpublishable.
func segmentKinds(compact string) ([]string, bool) {
	runes := []rune(compact)
	if len(runes) == 0 || len(runes) > maxCoverWordRunes*maxCoverSegments {
		return nil, false
	}
	segments := make([]int, len(runes)+1)
	parentWord := make([]string, len(runes)+1)
	parentAt := make([]int, len(runes)+1)
	for index := range segments {
		segments[index] = -1
		parentAt[index] = -1
	}
	segments[0] = 0
	for position := 0; position <= len(runes); position++ {
		if segments[position] < 0 || segments[position] >= maxCoverSegments {
			continue
		}
		limit := len(runes) - position
		if limit > maxCoverWordRunes {
			limit = maxCoverWordRunes
		}
		for length := 1; length <= limit; length++ {
			word := string(runes[position : position+length])
			_, known := lookupWord(word)
			if !known {
				continue
			}
			next := position + length
			if segments[next] >= 0 && segments[next] <= segments[position]+1 {
				continue
			}
			segments[next] = segments[position] + 1
			parentWord[next] = word
			parentAt[next] = position
		}
	}
	if segments[len(runes)] < 0 {
		return nil, false
	}
	words := make([]string, 0, segments[len(runes)])
	for at := len(runes); at > 0; {
		word := parentWord[at]
		if word == "" {
			return nil, false
		}
		words = append([]string{word}, words...)
		at = parentAt[at]
	}
	// A country directly in front of a company-forming industry word names an
	// institution rather than a sector: "美国银行" is Bank of America, "印度航空" is
	// Air India. The sector reading of those words carries a longer suffix ("银行业").
	if namesCompany(words) {
		return nil, false
	}
	kinds := make([]string, 0, len(words))
	concrete := false
	for _, word := range words {
		kind, known := lookupWord(word)
		if !known || kind == "" {
			continue
		}
		if !genericFlowWords[word] {
			concrete = true
		}
		kinds = append(kinds, kind)
	}
	if len(kinds) == 0 || !concrete {
		return nil, false
	}
	return kinds, true
}

func lookupWord(word string) (string, bool) {
	if numericTenorPattern.MatchString(word) {
		return "", true
	}
	if entry, found := targetLexicon[word]; found {
		return entry.kind, true
	}
	// English labels reach here lowercased and space-free, so a plural folds onto the
	// singular vocabulary word ("semiconductors" → "semiconductor").
	if len(word) > 4 && strings.HasSuffix(word, "s") && isASCIIWord(word) {
		if entry, found := targetLexicon[strings.TrimSuffix(word, "s")]; found {
			return entry.kind, true
		}
	}
	return "", false
}

func isASCIIWord(word string) bool {
	for _, current := range word {
		if current > 127 {
			return false
		}
	}
	return true
}

// highestPriorityKind picks the kind that owns the label: a policy rate, an indicator
// or a flow object outranks the industry or commodity word inside it.
func highestPriorityKind(kinds []string) string {
	best, bestRank := "", -1
	for _, kind := range kinds {
		rank, known := kindPriority[kind]
		if !known {
			continue
		}
		if bestRank < 0 || rank < bestRank {
			best, bestRank = kind, rank
		}
	}
	return best
}

// regionWords scope an object by geography or economic area. They are qualifiers, so
// they can never make a label publishable, and a name that is not in this list
// ("马来亚", "粤芯", "平安") leaves a residue that rejects the label.
var regionWords = []string{
	"美国", "中国", "日本", "欧洲", "欧元区", "欧盟", "英国", "法国", "德国", "意大利",
	"加拿大", "澳大利亚", "新西兰", "俄罗斯", "乌克兰", "中东", "沙特", "伊朗", "伊拉克",
	"以色列", "印度", "印度尼西亚", "印尼", "巴西", "墨西哥", "阿根廷", "智利", "秘鲁",
	"韩国", "朝鲜", "越南", "泰国", "马来西亚", "菲律宾", "新加坡", "土耳其", "埃及",
	"南非", "尼日利亚", "委内瑞拉", "古巴", "巴基斯坦", "瑞士", "挪威", "瑞典", "荷兰",
	"西班牙", "波兰", "希腊", "葡萄牙", "奥地利", "比利时", "爱尔兰", "芬兰", "海湾",
	"全球", "世界", "国际", "国内", "海外", "境外", "境内", "亚太", "亚洲", "北美",
	"拉美", "拉丁美洲", "非洲", "新兴市场", "发达经济体", "发展中经济体",
	"红海", "黑海", "地中海", "波罗的海", "北海", "阿拉伯海", "亚丁湾", "几内亚湾",
	"好望角", "马六甲海峡", "霍尔木兹海峡", "苏伊士运河", "巴拿马运河", "台湾海峡",
	"南海", "东海", "黄海", "日本海",
}

// measureWords name a scope, a measure or an industry component of an object.
var measureWords = []string{
	"战略", "商业", "国家", "政府", "官方", "民间", "国有", "私营", "大型", "主要", "重点",
	"核心", "关键", "高端", "先进", "上游", "下游", "中游", "传统", "新兴", "现代", "绿色",
	"清洁", "可再生", "联邦基金", "公开市场", "固定", "浮息", "挂钩",
	"市场", "水平", "数据", "指标", "指数", "目标", "区间", "上限", "下限", "中枢", "板块",
	"行业", "业", "领域", "环节", "体系", "规模", "总量", "增速", "增幅", "增长率", "波动",
	"波动率", "期限", "就业", "增长", "情况", "端", "面", "兑", "封装", "零部件", "器件",
	"设备", "代工", "晶圆", "操作", "进出口", "量",
	// Commodity grade and quotation qualifiers, so "电池级碳酸锂价格" reads as a
	// concrete commodity object qualified by grade and session instead of residue.
	"电池级", "工业级", "早盘", "午盘", "中间价", "均价",
	// English qualifiers and product names, so "Gold Price" or "Copper Inventories"
	// stay vocabulary words instead of an unknown residue.
	"price", "prices", "rate", "rates", "yield", "yields", "export", "exports",
	"import", "imports", "supply", "demand", "inventory", "inventories", "stockpile",
	"stockpiles", "market", "markets", "sector", "sectors", "data", "level", "levels",
	"growth", "output", "capacity", "diesel", "gasoline", "fuel oil", "heating oil",
	"jet fuel", "naphtha", "natural gas", "lpg", "ethane", "propane", "bitumen",
}

var qualifierWords = append(append([]string{}, regionWords...), measureWords...)

var regionQualifierSet = wordSet(regionWords)

// openScopeWords are the regional words that describe a market breadth rather than a
// country, so they never form a company name: "全球航空" is global aviation, while
// "印度航空" is Air India.
var openScopeWords = wordSet([]string{
	"全球", "世界", "国际", "国内", "海外", "境外", "境内", "亚太", "亚洲", "北美",
	"拉美", "拉丁美洲", "非洲", "新兴市场", "发达经济体", "发展中经济体", "海湾",
})

// corporateFormTerms are industry words that name a listed company rather than a
// sector once a country sits directly in front of them: "美国银行" is Bank of America
// and "印度航空" is Air India. Sector words that regions legitimately scope
// (石油, 港口, 航运) are deliberately absent, and "美国银行业" stays a sector because
// it resolves to the longer vocabulary word 银行业.
var corporateFormTerms = wordSet([]string{
	"银行", "航空", "保险", "证券", "电信", "电讯", "铁路", "邮政",
})

// namesCompany reports whether a segmentation reads as a country plus a bare
// corporate industry word.
func namesCompany(words []string) bool {
	return len(words) == 2 && regionQualifierSet[words[0]] && !openScopeWords[words[0]] &&
		corporateFormTerms[words[1]]
}

func wordSet(words []string) map[string]bool {
	set := make(map[string]bool, len(words))
	for _, word := range words {
		set[Normalized(word)] = true
	}
	return set
}
