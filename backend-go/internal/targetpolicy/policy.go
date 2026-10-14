// Package targetpolicy is the single shared vocabulary that decides whether a
// non-tradable research target is a genuine macroeconomic or industry object.
//
// The event researcher is asked to name only macro indicators, policy/rate/FX
// objects, commodities and industry categories, but an LLM regularly returns a
// person, an institution, a news sentence or a futures contract instead. Both
// the writer-side verifier (jobs) and the read-side public API (httpapi) must
// use one vocabulary, otherwise a free-text label silently becomes a public
// "宏观/行业" target and pollutes the /targets list.
package targetpolicy

import (
	"regexp"
	"strings"
)

var (
	fourDigitYearPattern  = regexp.MustCompile(`(?:19|20)[0-9]{2}`)
	monthQualifierPattern = regexp.MustCompile(`[0-9]{1,2}月`)
	pureNumberPattern     = regexp.MustCompile(`^[0-9._:%+\-/]+$`)
)

// Kinds are the stable target_type values used by the public contract. Sector
// resolution itself lives in the master-data package because the authoritative
// industry taxonomy is stored there.
const (
	Economy        = "economy"
	InterestRate   = "interest_rate"
	FXRate         = "fx_rate"
	CommodityPrice = "commodity_price"
	SupplyVolume   = "supply_volume"
	Shipping       = "shipping"
	RiskAsset      = "risk_asset"
	Sector         = "sector"
)

// Normalized lower-cases text and removes every space, so an English
// master-data label ("Electric & Electronics") matches the compact label kept
// in the taxonomy cache ("electricelectronics").
func Normalized(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), ""))
}

// PlausibleName reports whether a free-text label can name a macro or industry
// target at all. It rejects news clauses, quoted speech, instrument month
// strings, person and organization identifiers and long prose before any
// keyword matching happens.
func PlausibleName(name string) bool {
	compact := Normalized(name)
	runes := []rune(compact)
	if len(runes) < 2 || len(runes) > 24 {
		return false
	}
	for _, current := range runes {
		if !allowedNameRune(current) {
			return false
		}
	}
	// A bare number, ratio or price string is never a target object.
	if pureNumberPattern.MatchString(compact) {
		return false
	}
	// Four-digit years and month qualifiers name a reporting period or a
	// contract month, not the economic object itself ("2028年6月期原油").
	if fourDigitYearPattern.MatchString(compact) || monthQualifierPattern.MatchString(compact) {
		return false
	}
	for _, marker := range clauseMarkers {
		if strings.Contains(compact, marker) {
			return false
		}
	}
	for _, marker := range personMarkers {
		if strings.Contains(compact, marker) {
			return false
		}
	}
	for _, marker := range personASCII {
		if containsBoundedASCII(compact, marker) {
			return false
		}
	}
	for _, marker := range institutionMarkers {
		if strings.Contains(compact, marker) {
			return false
		}
	}
	for _, marker := range institutionASCII {
		if containsBoundedASCII(compact, marker) {
			return false
		}
	}
	for _, marker := range instrumentPhrases {
		if strings.Contains(compact, marker) {
			return false
		}
	}
	for _, marker := range instrumentMarkers {
		if containsBoundedASCII(compact, marker) {
			return false
		}
	}
	// "2028年6月期原油" style contract months pair a month with a delivery
	// period; a genuine forecast horizon keeps 预期/周期 wording.
	if strings.Contains(compact, "月") && strings.Contains(compact, "期") &&
		!strings.Contains(compact, "预期") && !strings.Contains(compact, "周期") {
		return false
	}
	return true
}

// Classify is the single entry point both the writer-side verifier and the
// read-side API use: it returns the macro target type of a label, or "" when the
// label is a person, institution, instrument or free-text sentence instead of a
// macroeconomic or industry object.
func Classify(name string) (string, bool) {
	if !PlausibleName(name) {
		return "", false
	}
	if exactCompanyNames[Normalized(name)] {
		return "", false
	}
	return MacroKind(name)
}

func allowedNameRune(current rune) bool {
	switch {
	case current >= 0x4E00 && current <= 0x9FFF: // CJK unified ideograph
		return true
	case current >= '0' && current <= '9':
		return true
	case current >= 'a' && current <= 'z':
		return true
	default:
		return strings.ContainsRune("._:%/-+&", current)
	}
}

// clauseMarkers are action verbs, instrument qualifiers and headline fragments
// that prove the label is a news sentence rather than an economic object.
var clauseMarkers = []string{
	"表示", "称将", "或将", "预计", "据悉", "消息", "采访", "讲话", "谈话", "发声", "表态",
	"呼吁", "警告", "承诺", "重申", "敦促", "抗议", "提名", "上任", "辞职", "去世",
	"当选", "胜选", "连任", "会晤", "通话", "会谈", "访问", "视察", "参观",
	"决议", "决定", "峰会", "选举", "就职", "罢免", "调查", "诉讼", "起诉", "裁决", "判决",
	"批准", "生效", "签署", "颁布", "公布", "宣布", "延期", "暂停", "审议", "协议", "谈判", "预期",
	// Direction words describe what happened to a price, not the object itself.
	"走强", "走弱", "走高", "走低", "升值", "贬值", "上涨", "下跌", "上升", "下降",
	"攀升", "回落", "反弹", "承压", "飙升", "暴跌", "大涨", "大跌",
	"交货", "交割", "期货", "期权", "合约", "主力", "持仓", "未平仓", "看涨", "看跌",
	"股价", "股票", "个股", "市值", "涨停", "跌停", "交易量", "成交量", "成交额", "活跃度", "换手率",
	"said", "says", "calls", "callsfor", "urges", "warns", "predicts", "reported", "sources",
	// Vague placeholders the extractor uses when it cannot name the object.
	"相关举措", "有关事项", "相关行动", "有关措施", "unknown", "n/a",
	// Connectives and action verbs. A target names an object, so a label that
	// contains a particle or a verb is a headline fragment. None of these runes
	// appear inside a vocabulary term.
	"的", "了", "将", "已", "被", "让", "使", "从", "给", "或", "以及", "及其",
	"是否", "必要", "旨在", "彼此", "全部", "包括", "通过", "利用", "进行",
	"加强", "推动", "推进", "促进", "加快", "支持", "出台", "实施", "落实",
	"开展", "印发", "征集", "运出", "运抵", "飞往", "招聘", "回升", "有关", "相关",
	"一位", "多位", "数名", "若干",
}

// personMarkers catch named individuals. An individual is never a macro or
// industry object, and macro news very often names the speaker instead of the
// affected indicator.
var personMarkers = []string{
	"特朗普", "鲍威尔", "耶伦", "拜登", "马斯克", "贝森特", "沃勒", "沃什", "拉加德",
	"植田", "黑田", "奥尔斯", "埃尔多安", "普京", "泽连斯基", "习主席", "国务院总理",
	"特朗普政府", "财政部长", "美联储主席", "央行行长", "白宫国家经济委员会主任",
	// Roles and job titles. News output names the speaker or the affected person,
	// never the indicator, so any label carrying a role is dropped.
	"工程师", "学者", "律师", "分析师", "交易员", "负责人", "总裁", "董事长", "总经理",
	"经理", "发言人", "特使", "参议员", "部长", "首相", "总理", "总统", "国王", "女王",
	"副驾驶", "机长", "飞行员", "船员", "驾驶员",
}

// personASCII are the romanized surnames that appear in English research output;
// they use bounded matching so an ordinary word cannot trip them.
var personASCII = []string{
	"powell", "yellen", "geithner", "mnuchin", "bessent", "walsh", "warsh", "bullard",
	"mushbock", "gopinath", "draghi", "kuroda", "ueda", "trump", "biden", "musk",
	"elarjan", "reichensperger", "williams", "daly", "hasker", "schmid", "bostic", "barkin",
}

// institutionMarkers are central banks, ministries, statistical agencies,
// supranational bodies, exchanges and named companies. They are the actors a
// headline names, never the macroeconomic or industry object a target has to be.
// They run before keyword classification so "世界银行" cannot fall into the bank
// industry and "欧央行" cannot become a policy-rate object.
var institutionMarkers = []string{
	"美联储", "联邦储备", "储备系统", "公开市场委员会", "欧洲央行", "欧央行", "欧委会", "欧盟委员会",
	"日本央行", "英国央行", "英格兰银行", "中国人民银行", "人民银行", "央行", "日银", "理事会",
	"劳工统计局", "统计局", "海关总署", "劳工部", "商务部", "财政部", "白宫", "官员",
	"经济研究局", "世界银行", "货币基金", "国际能源署", "能源署", "欧佩克", "石油输出国组织",
	"联合国", "世贸组织", "贸发会议", "纳斯达克", "纽交所", "纽约证券交易所", "伦交所", "港交所",
	"上交所", "深交所", "英伟达", "台积电", "海力士", "三星电子", "博通", "超威", "阿斯麦",
	"沙特阿美", "中石油", "中石化", "壳牌", "道达尔", "埃克森", "雪佛龙", "洛克希德", "波音", "空客",
	// Corporate and institutional suffixes. A label that carries one of these names a
	// legal entity ("广汽本田汽车有限公司", "拉丁美洲和加勒比开发银行"), not a macro
	// or industry object, even when its tail is a real industry word.
	"公司", "集团", "股份", "银行间", "研究院", "研究所", "研究中心", "协会", "学会",
	"联盟", "基金会", "大学", "学院", "学校", "会议", "座谈会", "论坛", "峰会", "航班",
	"航班号", "机队", "工厂", "车间", "门店", "分公司", "事业部",
	// Named central, development and multilateral banks. Their names contain flow and
	// industry words ("储备", "开发", "银行"), so coverage alone would publish them.
	"储备银行", "人民银行", "开发银行", "世界银行", "英格兰银行", "国际清算银行", "货币基金组织",
}

// institutionASCII uses the same bounded rule as the instrument codes.
var institutionASCII = []string{
	"nasdaq", "nyse", "lse", "hkex", "sse", "szse", "imf", "nber", "bls", "opec",
	"iea", "broadcom", "qualcomm", "openai", "anthropic",
}

// instrumentMarkers are exchange, contract-month and spot/futures qualifiers
// that belong to the tradable-asset bucket. They are matched with bounded ASCII
// rules so "cme" cannot reject an unrelated Latin word.
var instrumentMarkers = []string{
	"lme", "comex", "cbot", "nymex", "cme", "sgx", "dce", "shfe", "ine", "gfex",
	"wti", "brent", "etf", "cffex",
}

// instrumentPhrases are CJK instrument qualifiers matched as plain substrings.
var instrumentPhrases = []string{
	"现货", "期货", "期权", "合约", "主力", "持仓", "未平仓", "交割", "交货", "交割月",
	"近月", "远月", "月期", "连续", "连一", "连二", "升水", "贴水", "伦敦金", "伦敦银", "上期所",
	"大商所", "郑商所", "沪铜", "沪铝", "沪锌", "沪镍", "沪锡", "伦铜", "伦铝", "伦镍",
	"伦锌", "伦锡", "股价", "涨跌", "成交额", "成交量", "布伦特", "西德克萨斯", "阿曼原油",
	"stockprice", "shareprice", "stocks", "shares", "stockfuture",
}

// exactCompanyNames are companies whose full Chinese name is itself a readable
// macro or industry phrase, so neither the markers nor the coverage rule rejects
// them: "英国石油" is BP, yet it segments into the country and the sector word that
// "英国石油进口" and "英国天然气" legitimately use. They are matched as the whole label
// so a scoped object reading of the same words stays publishable.
var exactCompanyNames = map[string]bool{
	Normalized("英国石油"): true,
}

// MacroKind classifies a plausible label into one macro target type by segmenting the
// whole label into vocabulary words. Industry names resolved through master data are
// part of the same lexicon; the coverage rule is what stops a news clause or an
// institution that merely contains a keyword from becoming a target.
func MacroKind(name string) (string, bool) {
	compact := Normalized(name)
	if compact == "" {
		return "", false
	}
	kinds, covered := coveredKinds(compact)
	if !covered {
		return "", false
	}
	kind := highestPriorityKind(kinds)
	if kind == "" {
		return "", false
	}
	return kind, true
}

type kindCheck struct {
	kind         string
	tokens       []string
	boundedASCII bool
}

// FlowKind reports whether the kind describes a flow or quantity object (export
// bans, quotas, freight rates, policy rates and FX). Those outrank an industry
// name so "柴油出口" stays a supply observation instead of an oil industry.
func FlowKind(kind string) bool {
	switch kind {
	case InterestRate, FXRate, SupplyVolume, Shipping:
		return true
	default:
		return false
	}
}

// ImpactChannelAndRelationship maps an observation kind onto the public
// transmission channel and target-relation vocabulary.
func ImpactChannelAndRelationship(kind string) (string, string) {
	switch kind {
	case SupplyVolume:
		return "supply", "macro_indicator"
	case Shipping:
		return "cost", "market_exposure"
	case Economy:
		return "demand", "macro_indicator"
	case Sector:
		return "supply", "sector_exposure"
	case RiskAsset:
		return "risk_premium", "market_exposure"
	default:
		return "risk_premium", "macro_indicator"
	}
}

// containsBoundedASCII matches an abbreviation only when it is not embedded in a
// longer Latin word, so "cpi" matches "美国CPI" but never "optical".
func containsBoundedASCII(compact, token string) bool {
	for start := 0; ; {
		offset := strings.Index(compact[start:], token)
		if offset < 0 {
			return false
		}
		index, end := start+offset, start+offset+len(token)
		beforeOK := index == 0 || !isASCIILetter(rune(compact[index-1]))
		afterOK := end >= len(compact) || !isASCIILetter(rune(compact[end]))
		if beforeOK && afterOK {
			return true
		}
		start = end
	}
}

func isASCIILetter(current rune) bool {
	return current >= 'a' && current <= 'z'
}
