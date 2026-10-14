package targetpolicy

import "testing"

func TestClassifyAcceptsMacroAndIndustryObjects(t *testing.T) {
	cases := map[string]string{
		"美国CPI":          Economy,
		"CPI":            Economy,
		"非农就业":           Economy,
		"制造业PMI":         Economy,
		"联邦基金利率目标":       InterestRate,
		"国债收益率":          InterestRate,
		"10年期国债收益率":      InterestRate,
		"DR007":          InterestRate,
		"美元兑人民币":         FXRate,
		"日元":             FXRate,
		"美元指数":           FXRate,
		"柴油出口":           SupplyVolume,
		"原油出口禁令":         SupplyVolume,
		"铜库存":            SupplyVolume,
		"集装箱运价":          Shipping,
		"霍尔木兹海峡航运":       Shipping,
		"半导体":            Sector,
		"半导体行业":          Sector,
		"芯片":             Sector,
		"Semiconductors": Sector,
		"零售板块":           Sector,
		"白酒":             Sector,
		"国防军工":           Sector,
		"人工智能":           Sector,
		"银行":             Sector,
		"黄金价格":           CommodityPrice,
		"原油":             Sector,
		"碳酸锂价格":          CommodityPrice,
		"电池级碳酸锂价格":       CommodityPrice,
		"工业级碳酸锂":         CommodityPrice,
		"电池":             Sector,
		"电池行业":           Sector,
		"锂电池":            Sector,
		"动力电池":           Sector,
		"能源金属":           Sector,
		"锂矿":             Sector,
		"美国股市":           RiskAsset,
		"加密货币":           RiskAsset,
		"美国经济":           Economy,
		"欧元区经济":          Economy,
	}
	for name, expected := range cases {
		kind, ok := Classify(name)
		if !ok {
			t.Errorf("Classify(%q) rejected a valid macro/industry object", name)
			continue
		}
		if kind != expected {
			t.Errorf("Classify(%q) = %s, want %s", name, kind, expected)
		}
	}
}

func TestClassifyRejectsNonTargetLabels(t *testing.T) {
	rejected := []string{
		// People and offices.
		"特朗普", "鲍威尔", "黄立", "美联储主席鲍威尔", "Powell", "Jerome H. Powell",
		"美国总统特朗普",
		// Instruments and quotes.
		"LME锡", "COMEX黄金", "现货黄金", "布伦特原油", "WTI原油", "2028年6月期原油",
		"纳斯达克", "道琼斯工业平均指数", "标普500指数", "上证指数", "AAPL", "Tesla stock price",
		"沪镍主力合约", "伦敦金",
		// News sentences and free text.
		"美国劳工统计局发布的非农就业数据表明劳动力市场仍然紧张", "鲍威尔表示通胀风险仍然存在",
		"特朗普当选将推动减税放松监管", "消息人士称OPEC+考虑增产", "某公司宣布重组计划",
		"政策不确定性上升", "市场情绪好转", "需求疲软",
		// Too short or empty.
		"铜", "金", "", "   ",
	}
	for _, name := range rejected {
		if kind, ok := Classify(name); ok {
			t.Errorf("Classify(%q) = %s, want rejection", name, kind)
		}
	}
}

func TestFlowKindOutranksIndustryName(t *testing.T) {
	for _, name := range []string{"柴油出口", "原油出口禁令", "集装箱运价", "日元", "联邦基金利率目标"} {
		kind, ok := Classify(name)
		if !ok {
			t.Fatalf("Classify(%q) rejected a flow object", name)
		}
		if !FlowKind(kind) {
			t.Errorf("Classify(%q) = %s, want a flow kind so the industry word cannot win", name, kind)
		}
	}
	for _, name := range []string{"半导体", "白酒", "国防军工"} {
		kind, _ := Classify(name)
		if FlowKind(kind) {
			t.Errorf("Classify(%q) = %s, want an industry kind", name, kind)
		}
	}
}

func TestIndustryNameFoldsSuffixVariants(t *testing.T) {
	cases := map[string]string{
		"半导体":            "半导体",
		"半导体行业":          "半导体",
		"芯片":             "半导体",
		"Semiconductors": "半导体",
		"零售板块":           "零售",
		"白酒":             "食品饮料",
		"新能源汽车":          "汽车",
		"锂电池":            "电池",
		"动力电池":           "电池",
		"电池行业":           "电池",
		"锂矿":             "能源金属",
	}
	for name, expected := range cases {
		label, ok := IndustryName(name)
		if !ok || label != expected {
			t.Errorf("IndustryName(%q) = %q/%v, want %q", name, label, ok, expected)
		}
	}
	for _, name := range []string{"政策不确定性", "市场情绪", "某公司", "特朗普"} {
		if label, ok := IndustryName(name); ok {
			t.Errorf("IndustryName(%q) = %q, want no industry", name, label)
		}
	}
}

func TestMacroKindIgnoresAbbreviationsInsideWords(t *testing.T) {
	// "cpi" must not match inside a Latin word, and neither may "usd".
	if kind, ok := Classify("Optical Instruments"); ok {
		t.Errorf("Classify(\"Optical Instruments\") = %s, want rejection", kind)
	}
	if kind, ok := Classify("Marine Terminal"); ok {
		t.Errorf("Classify(\"Marine Terminal\") = %s, want rejection", kind)
	}
	if kind, ok := Classify("USD/CNY"); !ok || kind != FXRate {
		t.Errorf("Classify(\"USD/CNY\") = %s/%v, want %s", kind, ok, FXRate)
	}
}

// TestClassifyRejectsPublishedJunk covers the labels that were live on
// GET /api/v1/target-changes?kind=macro: news clauses, central-bank and agency
// names, exchange-specific quotes and contract-month strings.
func TestClassifyRejectsPublishedJunk(t *testing.T) {
	rejected := []string{
		"美国利率决议", "欧央行三大利率", "欧洲央行利率决议", "日银", "日银理事", "日美关税协议",
		"日美官员", "日美货币谈判", "美联储高利率", "美国强就业", "美元兑日元汇率预期",
		"美元走弱", "关税暂停期", "对等关税10%延期至2026年11月", "特朗普关税",
		"特朗普关税政策不确定性", "美国总统特朗普关税政策", "日本央行政策利率变动",
		"日本央行加息", "欧佩克产量", "欧佩克+产量政策", "国际能源署释放储备",
		"LME铜价", "LME铜现货结算价", "COMEX黄金", "伦敦金期货价格", "沪铜期货价格",
		"纳斯达克", "道琼斯工业平均指数", "标普500指数", "上证指数", "特朗普当选概率",
	}
	for _, name := range rejected {
		if kind, ok := Classify(name); ok {
			t.Errorf("Classify(%q) = %s, want rejection", name, kind)
		}
	}
}

func TestImpactChannelAndRelationshipCoverEveryKind(t *testing.T) {
	allowedChannels := map[string]bool{
		"supply": true, "demand": true, "revenue": true, "cost": true,
		"profit": true, "cash_flow": true, "valuation": true, "risk_premium": true,
	}
	allowedRelationships := map[string]bool{
		"sector_exposure": true, "asset_issuer": true, "direct": true, "indirect": true,
		"market_exposure": true, "macro_indicator": true,
	}
	for _, kind := range []string{Economy, InterestRate, FXRate, CommodityPrice, SupplyVolume, Shipping, RiskAsset, Sector} {
		channel, relationship := ImpactChannelAndRelationship(kind)
		if !allowedChannels[channel] {
			t.Errorf("ImpactChannelAndRelationship(%q) channel %q is not an allowed impact channel", kind, channel)
		}
		if !allowedRelationships[relationship] {
			t.Errorf("ImpactChannelAndRelationship(%q) relationship %q is not an allowed relation", kind, relationship)
		}
	}
}
