package targetpolicy

// macroKindChecks is ordered: a policy-rate or indicator hit always outranks a
// looser market or industry word, so "国债收益率" is an interest-rate object even
// though it also mentions a market.
var macroKindChecks = []kindCheck{
	{kind: InterestRate, tokens: []string{"加息", "降息", "利率", "收益率", "国债", "国库券", "票据", "回购", "贷款", "存款", "准备金", "流动性", "公开市场操作", "量化宽松", "缩表", "信贷", "美债", "美元债", "rate", "rates", "yield", "yields"}},
	{kind: InterestRate, tokens: []string{"lpr", "mlf", "slf", "psl", "shibor", "libor", "hibor", "yibor", "mibor", "omo", "rrp", "omp", "qe", "mli", "cdi", "sar", "far", "psb", "qrre", "tmlf", "slo", "sfsl", "pfsl", "dr001", "dr007", "dr014", "fdr001", "fdr007", "r001", "r007", "sofr", "estr", "sonia", "euribor", "effr", "ffr", "iorb", "sobr", "ntl"}, boundedASCII: true},
	{kind: Economy, tokens: []string{"cpi", "ppi", "gdp", "pmi", "pce", "m1", "m2", "m0", "m3", "社融", "通胀", "通缩", "物价", "消费者信心", "消费者信", "信心指数", "景气指数", "失业率", "非农", "新增就业", "就业市场", "就业人数", "就业数据", "就业增长", "就业报告", "就业形势", "工资", "零售销售", "经济", "衰退", "停滞", "财政", "预算", "关税", "出口管制", "制裁", "禁运", "政府停摆", "信用评级", "评级上调", "评级下调", "外汇储备", "逆回购", "买断式", "国库单一账户", "特别国债", "特别市政债", "专项债", "专项票据", "特别提款权", "内需", "货币贬值", "货币升值", "汇率干预", "去美元化", "资本要求", "巴塞尔"}},
	{kind: Economy, tokens: []string{"gdp", "cpi", "ppi", "pmi", "pce", "cai", "cerf", "cdi", "mibor", "hibor", "yibor", "sfb"}, boundedASCII: true},
	{kind: FXRate, tokens: []string{"汇率", "美元指数", "美元", "欧元", "日元", "英镑", "瑞郎", "瑞士法郎", "加元", "澳元", "人民币", "离岸人民币", "卢布", "里拉", "卢比", "印尼卢比", "比索", "韩元", "新台币", "林吉特", "新元", "越南盾", "泰铢", "雷亚尔", "兰特", "新西兰元", "土耳其里拉", "阿根廷比索", "现钞", "现汇", "兑美元", "兑人民币", "货币汇率"}},
	{kind: FXRate, tokens: []string{"usd", "cny", "eur", "jpy", "gbp", "chf", "cad", "aud", "nzd", "rub", "inr", "krw", "twd", "myr", "idr", "vnd", "thb", "brl", "zar", "mxn", "try", "ars", "hkd", "sgd", "aed", "chyn", "chyc"}, boundedASCII: true},
	{kind: SupplyVolume, tokens: []string{"产量", "出口", "进口", "供应", "供给", "库存", "储备", "产能", "开工率", "停产", "检修", "投放", "拍卖", "招标", "中标", "配额", "许可证", "禁令", "限制", "增持", "减持", "释放", "贸易", "补贴", "export", "exports", "import", "imports", "supply", "demand", "inventory", "inventories", "stockpile", "stockpiles", "output", "capacity"}},
	{kind: Shipping, tokens: []string{"港口", "港口运营", "港口使用", "港口拥堵", "集装箱", "海运", "航运", "船舶", "轮船", "油轮", "运价", "运量", "运量指数", "航线", "吞吐量", "干散货", "运费", "货运", "货舱"}},
	{kind: RiskAsset, tokens: []string{"股市", "股票市场", "证券市场", "资本市场", "金融市场", "数字资产", "加密货币", "虚拟货币", "加密资产", "稳定币", "风险资产", "股票基金", "风险偏好", "A股", "美股", "港股", "股指"}},
	{kind: Sector, tokens: []string{"人工智能", "算力", "机器人", "具身智能", "稀土", "小金属", "新能源", "半导体", "芯片", "油价", "原油", "成品油", "柴油", "汽油", "煤制油", "煤炭", "天然气", "电力", "公用事业", "国防", "军工", "航天", "航空", "汽车", "银行", "保险", "券商", "房地产", "医药", "创新药", "白酒", "食品饮料", "家电", "消费电子", "光伏", "储能", "风电", "钢铁", "水泥", "化工", "港口", "航运", "物流", "快递", "农业", "养殖", "猪周期", "旅游", "免税", "游戏", "传媒", "云计算", "软件", "网络安全"}},
	{kind: CommodityPrice, tokens: []string{"黄金", "白银", "铜", "铝", "锌", "镍", "锡", "锂", "钴", "钨", "铂", "钯", "铀", "铁矿石", "钢材", "螺纹", "橡胶", "棉花", "白糖", "大豆", "玉米", "小麦", "棕榈油", "原油", "石油", "天然气", "煤炭", "焦炭", "焦煤", "甲醇", "多晶硅", "碳酸锂", "价格", "牛肉", "猪肉", "乳制品"}},
	{kind: CommodityPrice, tokens: []string{"gold", "silver", "copper", "bauxite", "uranium", "ironore", "brent", "wti", "crude", "natgas", "lng", "soybean", "soymeal", "soyoil", "palm", "rapeseed", "canola", "sunflower", "olive", "maize", "sugar", "coffee", "cocoa", "cotton", "rice", "rubber", "potash", "urea", "diammonium", "phosphate", "tallow", "timber", "lumber", "ore", "concentrate", "carbonate", "polysilicon", "coke", "coal", "gas", "crudeoil"}, boundedASCII: true},
}
