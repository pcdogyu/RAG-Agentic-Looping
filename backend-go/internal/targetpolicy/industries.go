package targetpolicy

// IndustryName resolves an industry or sector word to the canonical Chinese
// label used by the industry master data. Master data stays authoritative in
// the read path; this table only decides whether a free-text label names an
// industry at all, and folds obvious suffix variants ("半导体行业" → "半导体") onto
// one label so one industry is not published under several names.
func IndustryName(name string) (string, bool) {
	compact := Normalized(name)
	if len([]rune(compact)) < 2 {
		return "", false
	}
	if label, found := industryAliasIndex[compact]; found {
		return label, true
	}
	// Longest prefix match keeps "零售行业" and "半导体板块" on the canonical label
	// while never matching an arbitrary longer phrase.
	runes := []rune(compact)
	for length := len(runes) - 1; length >= 2; length-- {
		if label, found := industryAliasIndex[string(runes[:length])]; found {
			return label, true
		}
	}
	return "", false
}

type industryEntry struct {
	label   string
	aliases []string
}

var industryAliasIndex map[string]string

var industryEntries = []industryEntry{
	{label: "信息技术", aliases: []string{"科技", "technology"}},
	{label: "公用事业", aliases: []string{"电力公用事业", "utilities"}},
	{label: "医疗健康", aliases: []string{"医药生物", "医疗保健", "healthcare"}},
	{label: "原材料", aliases: []string{"材料", "基础化工", "industrial materials"}},
	{label: "可选消费", aliases: []string{"非必需消费", "consumer cyclical"}},
	{label: "工业", aliases: []string{"制造业", "工业制造", "industrials"}},
	{label: "必选消费", aliases: []string{"日常消费", "consumer defensive"}},
	{label: "房地产", aliases: []string{"地产", "real estate"}},
	{label: "数字资产", aliases: []string{"加密资产", "加密货币", "数字货币", "区块链", "crypto", "cryptocurrency"}},
	{label: "能源", aliases: []string{"石油天然气", "oil gas"}},
	{label: "通信服务", aliases: []string{"传媒通信", "communication services"}},
	{label: "金融", aliases: []string{"金融服务", "金融业", "financial services"}},
	{label: "互联网服务", aliases: []string{"互联网", "网络服务", "internet services", "internet content"}},
	{label: "传媒娱乐", aliases: []string{"文化传媒", "文化传播", "媒体及娱乐", "游戏", "传媒", "media", "entertainment", "advertising"}},
	{label: "电信", aliases: []string{"电信运营", "电讯", "telecom", "telecommunications"}},
	{label: "家居与家电", aliases: []string{"家用电器", "家电", "家居用品", "家庭电器", "household appliances", "home improvement"}},
	{label: "汽车", aliases: []string{"汽车整车", "汽车制造", "新能源车", "新能源汽车", "automotive", "automobile"}},
	{label: "汽车零部件", aliases: []string{"汽车配件", "auto parts"}},
	{label: "消费服务", aliases: []string{"教育", "consumer services", "personal services"}},
	{label: "纺织服饰", aliases: []string{"纺织服装", "服装家纺", "纺织业", "奢侈品", "apparel", "textile"}},
	{label: "零售", aliases: []string{"商业百货", "批发业", "零售业", "电商", "specialty retail", "department stores", "discount stores", "grocery stores"}},
	{label: "餐饮旅游与休闲", aliases: []string{"旅游酒店", "旅游", "餐饮", "酒店", "休闲", "restaurants", "travel services", "leisure"}},
	{label: "农业", aliases: []string{"农牧饲渔", "农林牧渔", "种植业", "畜牧业", "养殖业", "农产品", "agriculture", "farm products"}},
	{label: "家庭与个人用品", aliases: []string{"日用化工", "美容护理", "烟草", "personal products"}},
	{label: "必选消费零售", aliases: []string{"消费者主要零售商", "商贸零售", "consumer staples retail"}},
	{label: "食品饮料", aliases: []string{"酿酒行业", "食品加工", "饮料乳品", "白酒", "啤酒", "食品", "饮料", "beverages", "packaged foods"}},
	{label: "新能源", aliases: []string{"光伏设备", "光伏", "风电设备", "风电", "储能", "氢能", "renewable energy", "solar", "clean energy"}},
	{label: "电池", aliases: []string{"锂电池", "动力电池", "锂电", "电池制造", "battery", "batteries"}},
	{label: "能源金属", aliases: []string{"锂矿", "锂盐"}},
	{label: "煤炭", aliases: []string{"焦煤", "焦炭", "coal"}},
	{label: "石油与天然气", aliases: []string{"石油及天然气", "油气", "石油", "原油", "天然气", "成品油", "柴油", "汽油", "oil gas", "crude", "brent", "wti"}},
	{label: "保险", aliases: []string{"再保险", "insurance"}},
	{label: "信贷服务", aliases: []string{"消费金融", "抵押贷款", "credit services", "mortgage"}},
	{label: "综合金融", aliases: []string{"多元金融", "其他金融"}},
	{label: "资产管理", aliases: []string{"基金管理", "asset management", "investment managers"}},
	{label: "资本市场", aliases: []string{"证券行业", "证券", "券商", "投行", "brokerage", "capital markets", "investment banking"}},
	{label: "银行", aliases: []string{"银行业", "商业银行", "banking", "banks"}},
	{label: "制药", aliases: []string{"医药商业", "化学制药", "中药", "药品", "医药", "创新药", "pharmaceuticals", "drug manufacturers"}},
	{label: "医疗器械", aliases: []string{"医疗设备", "medical devices", "medical instruments"}},
	{label: "医疗服务", aliases: []string{"医疗美容", "卫生", "healthcare services", "healthcare providers"}},
	{label: "生物科技", aliases: []string{"生物制品", "biotechnology", "biotech"}},
	{label: "交通运输", aliases: []string{"运输仓储", "航空机场", "铁路公路", "航空公司", "铁路", "航空", "机场", "transportation", "airlines", "railroads"}},
	{label: "商业与专业服务", aliases: []string{"科研服务", "商务服务", "专业服务", "商业服务", "环保行业", "环保", "business services", "environmental services"}},
	{label: "建筑与工程", aliases: []string{"工程建设", "装修装饰", "建筑", "建筑工程", "construction"}},
	{label: "机械设备", aliases: []string{"工程机械", "通用设备", "专用设备", "机械行业", "工业设备", "机器人", "machinery", "industrial machinery"}},
	{label: "电气设备", aliases: []string{"电网设备", "电机", "电源设备", "electrical equipment"}},
	{label: "航空航天与国防", aliases: []string{"航空航天", "航天航空", "航空工业", "国防军工", "军工", "卫星制造", "航天", "国防", "aerospace", "defense"}},
	{label: "航运与物流", aliases: []string{"航运港口", "航运", "物流", "港口", "海运", "shipping", "logistics", "freight"}},
	{label: "信息技术服务", aliases: []string{"数据中心", "算力", "数字化服务", "云计算", "it services", "data center"}},
	{label: "半导体", aliases: []string{"芯片", "集成电路", "半导体设备", "semiconductors", "semiconductor"}},
	{label: "电子元件", aliases: []string{"消费电子", "光学光电子", "电子元件", "电子设备", "electronic components", "consumer electronics"}},
	{label: "硬件与设备", aliases: []string{"计算机设备", "通信设备", "电脑硬件", "硬件", "hardware", "computer equipment"}},
	{label: "软件", aliases: []string{"软件服务", "软件开发", "人工智能", "saas", "software"}},
	{label: "化工", aliases: []string{"化学原料", "化学制品", "化纤", "塑料制品", "橡胶制品", "化肥", "农药", "chemicals", "chemical"}},
	{label: "建筑材料", aliases: []string{"水泥建材", "装修建材", "玻璃玻纤", "水泥", "construction materials", "building materials"}},
	{label: "造纸与林业", aliases: []string{"造纸印刷", "包装材料", "造纸", "林业", "paper", "forest products"}},
	{label: "金属与采矿", aliases: []string{"金属制品", "黄金及贵金属", "贵金属", "有色金属", "小金属", "稀土", "采掘", "采矿", "钢铁", "黄金", "白银", "铜", "铝", "metals mining", "steel", "gold", "silver", "copper", "aluminum"}},
	{label: "房地产服务", aliases: []string{"物业服务", "real estate services"}},
	{label: "房地产投资信托", aliases: []string{"房地产信托", "reits", "reit"}},
	{label: "燃气与水务", aliases: []string{"水电煤气", "燃气", "水务", "water utilities", "gas utilities"}},
	{label: "电力", aliases: []string{"电力行业", "火力发电", "核电", "electric utilities", "power generation"}},
}

func init() {
	industryAliasIndex = make(map[string]string, 512)
	for _, entry := range industryEntries {
		for _, term := range append([]string{entry.label}, entry.aliases...) {
			key := Normalized(term)
			if key == "" {
				continue
			}
			if _, exists := industryAliasIndex[key]; !exists {
				industryAliasIndex[key] = entry.label
			}
		}
	}
}
