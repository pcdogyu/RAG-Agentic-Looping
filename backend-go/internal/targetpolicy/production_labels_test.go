package targetpolicy

import "testing"

// TestClassifyAcceptsObjectsFromProductionResearch pins the labels that production
// research legitimately produces. They must survive the coverage rule, otherwise the
// sweep and the read filter would hide real macro and industry observations.
func TestClassifyAcceptsObjectsFromProductionResearch(t *testing.T) {
	cases := map[string]string{
		"柴油": Sector, "柴油储备": SupplyVolume, "柴油出口限制": SupplyVolume, "进口柴油": SupplyVolume,
		"柴油供应": SupplyVolume, "铜库存": SupplyVolume, "铜产量": SupplyVolume, "牛肉进口": SupplyVolume,
		"战略石油储备": SupplyVolume, "电力供应": SupplyVolume, "固定利率逆回购操作": InterestRate,
		"半导体封装材料": Sector, "英国GDP增长": Economy, "英国天然气": Sector, "A股市场": RiskAsset,
		"美债收益率": InterestRate, "霍尔木兹海峡航运": Shipping, "苏伊士运河航运": Shipping,
		"集装箱运价": Shipping, "制造业PMI": Economy, "10年期国债收益率": InterestRate,
		"联邦基金利率目标": InterestRate, "美元/人民币": FXRate, "黄金价格": CommodityPrice,
		"Semiconductors": Sector, "Gold Price": CommodityPrice, "Copper Inventories": SupplyVolume,
		// A region in front of a sector word stays publishable; the company reading
		// needs the bare corporate word, which is rejected below.
		"美国银行业": Sector, "印度银行业": Sector, "航空业": Sector, "俄罗斯石油": CommodityPrice,
		"伊朗港口": Shipping, "中国半导体": Sector, "中国港口": Shipping, "全球航空": Sector,
		"国内航线": Shipping, "亚洲航空货运": Shipping,
	}
	for name, expected := range cases {
		kind, ok := Classify(name)
		if !ok {
			t.Errorf("Classify(%q) rejected a valid object", name)
			continue
		}
		if kind != expected {
			t.Errorf("Classify(%q) = %s, want %s", name, kind, expected)
		}
	}
}

// TestClassifyRejectsPublishedMacroJunk pins the labels that were live on
// GET /api/v1/target-changes?kind=macro&scope=observed in production: airline and
// company names, engineers and co-pilots, news clauses and quantities. Each one
// contains a real keyword, so only the full-coverage rule rejects them.
func TestClassifyRejectsPublishedMacroJunk(t *testing.T) {
	rejected := []string{
		// Airlines, carriers and their staff.
		"迪拜航空的副驾驶", "迪拜航空FZ1073航班涉事副驾驶", "迪拜航空的专业和安全招聘要求",
		"科威特航空公司", "15条国际航空货运航线", "国内航线客票燃油附加费",
		// Companies, banks and research institutes.
		"马来亚银行", "平安数字银行", "粤芯半导体", "印度国家电力公司", "日本关西电力公司",
		"拉丁美洲和加勒比开发银行", "中国汽车技术研究中心有限公司", "广汽本田汽车有限公司",
		"深圳市睿嘉汽车有限公司", "委内瑞拉国家石油公司PDVSA负责人", "Terrafab工厂将重塑芯片制造",
		// People and roles.
		"一位负责整车运动控制的资深汽车工程师", "多位汽车研发工程师和学者", "美俄领导人关于柴油供应的共识",
		"原油投机者", "沙特防空拦截弹库存",
		// News clauses and quantities.
		"是否有必要额外释放柴油", "利用所有合法权限来进行与俄罗斯的柴油交易", "与俄罗斯的柴油交易",
		"四部门加强汽车产品创新设计和研发测试验证有关管理", "30064辆汽车",
		"昨天从霍尔木兹海峡运出了2800万桶石油", "约146万桶日的原油", "105份石油合同",
		"三项社会保险基金累计结余112万亿元", "俄罗斯向美国及全球市场提供柴油", "俄罗斯柴油贸易额创新高",
		"包括俄罗斯燃料供应在内的大规模经济倡议", "适应人工智能发展促就业行动", "特定垂直电力传输系统及其组成部分和下游计算机系统",
		"原产于或进口自中国的二环己基碳二亚胺", "日常石油产量和日常天然气产量", "石油和石油产品",
		"第二十届全国半导体与集成技术会议", "全国农业科技工作会议", "煤炭安全稳产稳供工作推进会",
		"出口管制对话机制", "出口管制领域彼此关切", "电商消费品牌", "能源停火", "纽约原油暗盘",
		"葛洲坝航运扩能",
		// Named institutions: region plus a corporate industry word, or a bank whose
		// name is built from vocabulary words.
		"美国银行", "瑞士银行", "印度航空", "俄罗斯银行", "澳大利亚储备银行", "国家开发银行",
		"世界银行", "中国人民银行",
		// A bare flow word never names an object.
		"库存", "出口", "价格",
	}
	for _, name := range rejected {
		if kind, ok := Classify(name); ok {
			t.Errorf("Classify(%q) = %s, want rejection", name, kind)
		}
	}
}
