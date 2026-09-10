export const analystEvidenceTypes = [
	"forecast_assumption",
	"valuation_multiple",
	"cost_of_capital",
	"benchmark_expectation",
	"rating_rationale",
	"invalidation_rule",
] as const;

export type AnalystEvidenceType = (typeof analystEvidenceTypes)[number];

export const analystEvidenceTypeLabels: Record<AnalystEvidenceType, string> = {
	forecast_assumption: "预测假设",
	valuation_multiple: "估值倍数",
	cost_of_capital: "资本成本",
	benchmark_expectation: "基准预期",
	rating_rationale: "评级理由",
	invalidation_rule: "论点失效规则",
};

export type AnalystEvidencePreview = {
	body: Record<string, unknown>;
	evidenceType: AnalystEvidenceType;
	title: string;
	valueSummary: string;
	availableAt: string;
};

export type WorkflowPreview = {
	body: Record<string, unknown>;
	asOf: string;
	snapshotCount: number;
	assumptionCount: number;
	dcfScenarioCount: number;
	multipleScenarioCount: number;
	evidenceIDs: string[];
};

export type SchedulePreview = {
	body: Record<string, unknown>;
	forecastVersionID: string;
	cadenceHours: number;
	maxPriceAgeHours: number;
	maxPlanAgeDays: number;
	dcfScenarioCount: number;
	multipleScenarioCount: number;
	evidenceIDs: string[];
};

const forecastFields = new Set([
	"revenue_growth", "revenue_delta", "operating_margin_delta", "tax_rate_delta",
	"capex_delta", "change_nwc_delta", "diluted_shares_delta",
]);
const invalidationOperators = new Set(["missing", "changed", "eq", "lt", "lte", "gt", "gte"]);

function objectValue(value: unknown, message: string): Record<string, unknown> {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(message);
	return value as Record<string, unknown>;
}

function rejectUnknown(value: Record<string, unknown>, allowed: readonly string[], context: string) {
	const supported = new Set(allowed);
	const extra = Object.keys(value).filter((field) => !supported.has(field));
	if (extra.length > 0) throw new Error(`${context} 包含不受支持字段：${extra.join("、")}`);
}

function parseObjectJSON(raw: string, message: string) {
	try {
		return objectValue(JSON.parse(raw) as unknown, message);
	} catch (error) {
		if (error instanceof Error && error.message === message) throw error;
		throw new Error(message);
	}
}

function requiredString(value: unknown, field: string) {
	const result = typeof value === "string" ? value.trim() : "";
	if (!result) throw new Error(`${field} 不能为空`);
	return result;
}

function finiteNumber(value: unknown, field: string) {
	if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${field} 必须是有限数值，缺失值请保持 null 而不是填 0`);
	return value;
}

function integerInRange(value: unknown, field: string, minimum: number, maximum: number) {
	const parsed = finiteNumber(value, field);
	if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`${field} 必须是 ${minimum}—${maximum} 的整数`);
	return parsed;
}

function cleanStringList(value: unknown, field: string, allowEmpty = false) {
	if (!Array.isArray(value)) throw new Error(`${field} 必须是字符串数组`);
	const result = [...new Set(value.map((item) => typeof item === "string" ? item.trim() : "").filter(Boolean))];
	if (!allowEmpty && result.length === 0) throw new Error(`${field} 不能为空`);
	return result;
}

function parseRFC3339(value: unknown, field: string, now: Date) {
	const text = requiredString(value, field);
	if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(text)) throw new Error(`${field} 必须是 RFC3339 时间`);
	const parsed = new Date(text);
	if (Number.isNaN(parsed.getTime())) throw new Error(`${field} 必须是有效 RFC3339 时间`);
	if (parsed.getTime() > now.getTime()) throw new Error(`${field} 不能位于未来`);
	return parsed;
}

function safeSourceURL(value: unknown) {
	const raw = requiredString(value, "source_url");
	try {
		const parsed = new URL(raw);
		if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error();
		parsed.username = "";
		parsed.password = "";
		parsed.search = "";
		parsed.hash = "";
		return parsed.toString();
	} catch {
		throw new Error("source_url 必须是绝对 HTTP/HTTPS 地址");
	}
}

function evidenceValuesTemplate(evidenceType: AnalystEvidenceType, benchmarkID: string) {
	switch (evidenceType) {
	case "forecast_assumption": return { field: "", value: null };
	case "valuation_multiple": return { selected_multiple: null };
	case "cost_of_capital": return { wacc: null };
	case "benchmark_expectation": return { benchmark_id: benchmarkID.trim(), expected_return: null };
	case "rating_rationale": return { reason_codes: [] };
	case "invalidation_rule": return { rule_type: "", operator: "", threshold: null };
	}
}

export function analystEvidenceTemplate(assetID: string, evidenceType: AnalystEvidenceType, benchmarkID = "") {
	if (!analystEvidenceTypes.includes(evidenceType)) return "";
	return JSON.stringify({
		asset_id: assetID.trim(), evidence_type: evidenceType, title: "", rationale: "",
		values: evidenceValuesTemplate(evidenceType, benchmarkID), observed_at: "", available_at: "",
		source_name: "", source_document_id: "", source_url: "", approved_by: "",
	}, null, 2);
}

export function analystEvidenceBody(assetID: string, raw: string, now = new Date()): AnalystEvidencePreview {
	if (Number.isNaN(now.getTime())) throw new Error("当前校验时间无效");
	const source = parseObjectJSON(raw, "分析师证据必须是有效 JSON 对象");
	rejectUnknown(source, ["asset_id", "evidence_type", "title", "rationale", "values", "observed_at", "available_at", "source_name", "source_document_id", "source_url", "approved_by"], "分析师证据");
	const canonicalAssetID = requiredString(assetID, "当前资产 ID");
	const submittedAssetID = requiredString(source.asset_id, "asset_id");
	if (submittedAssetID !== canonicalAssetID) throw new Error("asset_id 必须与当前标的一致");
	const evidenceType = requiredString(source.evidence_type, "evidence_type") as AnalystEvidenceType;
	if (!analystEvidenceTypes.includes(evidenceType)) throw new Error("evidence_type 不受支持");
	const title = requiredString(source.title, "title");
	if (title.length > 240) throw new Error("title 超过 240 字符");
	const rationale = requiredString(source.rationale, "rationale");
	const sourceName = requiredString(source.source_name, "source_name");
	const sourceDocumentID = requiredString(source.source_document_id, "source_document_id");
	const approvedBy = requiredString(source.approved_by, "approved_by");
	if (sourceName.length > 120 || sourceDocumentID.length > 320 || approvedBy.length > 160) throw new Error("来源、文档或批准人超过契约长度限制");
	const observedAt = parseRFC3339(source.observed_at, "observed_at", now);
	const availableAt = parseRFC3339(source.available_at, "available_at", now);
	if (availableAt.getTime() < observedAt.getTime()) throw new Error("available_at 不能早于 observed_at");
	const values = objectValue(source.values, "values 必须是非空 JSON 对象");
	if (Object.keys(values).length === 0) throw new Error("values 必须是非空 JSON 对象");
	let valueSummary = "";
	switch (evidenceType) {
	case "forecast_assumption": {
		const field = requiredString(values.field, "values.field");
		if (!forecastFields.has(field)) throw new Error("values.field 不是受支持的预测假设字段");
		const value = finiteNumber(values.value, "values.value");
		values.field = field; values.value = value; valueSummary = `${field}=${value}`;
		break;
	}
	case "valuation_multiple": {
		const selected = finiteNumber(values.selected_multiple, "values.selected_multiple");
		if (selected <= 0 || selected > 200) throw new Error("selected_multiple 必须大于 0 且不超过 200");
		values.selected_multiple = selected; valueSummary = `selected_multiple=${selected}`;
		break;
	}
	case "cost_of_capital": {
		const wacc = finiteNumber(values.wacc, "values.wacc");
		if (wacc <= 0 || wacc >= 1) throw new Error("wacc 必须位于 0 和 1 之间");
		values.wacc = wacc; valueSummary = `wacc=${wacc}`;
		break;
	}
	case "benchmark_expectation": {
		const benchmarkID = requiredString(values.benchmark_id, "values.benchmark_id");
		const expectedReturn = finiteNumber(values.expected_return, "values.expected_return");
		if (expectedReturn <= -1 || expectedReturn > 10) throw new Error("expected_return 必须大于 -1 且不超过 10");
		values.benchmark_id = benchmarkID; values.expected_return = expectedReturn; valueSummary = `${benchmarkID}=${expectedReturn}`;
		break;
	}
	case "rating_rationale": {
		const reasonCodes = cleanStringList(values.reason_codes, "values.reason_codes");
		values.reason_codes = reasonCodes; valueSummary = reasonCodes.join("、");
		break;
	}
	case "invalidation_rule": {
		const ruleType = requiredString(values.rule_type, "values.rule_type");
		values.rule_type = ruleType; valueSummary = ruleType;
		break;
	}
	}
	const body = {
		asset_id: canonicalAssetID, evidence_type: evidenceType, title, rationale, values,
		observed_at: observedAt.toISOString(), available_at: availableAt.toISOString(), source_name: sourceName,
		source_document_id: sourceDocumentID, source_url: safeSourceURL(source.source_url), approved_by: approvedBy,
	};
	return { body, evidenceType, title, valueSummary, availableAt: availableAt.toISOString() };
}

function validateFinancialInputs(value: unknown) {
	const inputs = objectValue(value, "forecast.inputs 必须是对象");
	rejectUnknown(inputs, ["currency", "unit", "revenue", "operating_margin", "tax_rate", "depreciation", "capex", "change_nwc", "diluted_shares"], "forecast.inputs");
	inputs.currency = requiredString(inputs.currency, "forecast.inputs.currency").toUpperCase();
	inputs.unit = requiredString(inputs.unit, "forecast.inputs.unit").toLowerCase();
	if (!new Set(["reported", "thousands", "millions", "billions"]).has(inputs.unit as string)) throw new Error("forecast.inputs.unit 不受支持");
	for (const field of ["revenue", "operating_margin", "tax_rate", "depreciation", "capex", "change_nwc", "diluted_shares"] as const) inputs[field] = finiteNumber(inputs[field], `forecast.inputs.${field}`);
	if ((inputs.revenue as number) < 0 || (inputs.diluted_shares as number) <= 0) throw new Error("revenue 不能为负且 diluted_shares 必须大于 0");
	if ((inputs.operating_margin as number) < -1 || (inputs.operating_margin as number) > 1 || (inputs.tax_rate as number) < 0 || (inputs.tax_rate as number) > 1) throw new Error("operating_margin 必须位于 -1—1，tax_rate 必须位于 0—1");
	return inputs;
}

function validateAssumptions(value: unknown) {
	if (!Array.isArray(value)) throw new Error("forecast.assumptions 必须是数组");
	return value.map((entry, index) => {
		const item = objectValue(entry, `forecast.assumptions[${index}] 必须是对象`);
		rejectUnknown(item, ["field", "value", "event_id", "evidence_ids", "condition", "fiscal_period_end", "approved"], `forecast.assumptions[${index}]`);
		const field = requiredString(item.field, `forecast.assumptions[${index}].field`);
		if (!forecastFields.has(field)) throw new Error(`forecast.assumptions[${index}].field 不受支持`);
		const result: Record<string, unknown> = { ...item, field, value: finiteNumber(item.value, `forecast.assumptions[${index}].value`), evidence_ids: cleanStringList(item.evidence_ids, `forecast.assumptions[${index}].evidence_ids`) };
		if (item.approved !== true) throw new Error(`forecast.assumptions[${index}].approved 必须明确为 true`);
		result.approved = true;
		if (item.event_id !== undefined) {
			if (typeof item.event_id !== "string") throw new Error(`forecast.assumptions[${index}].event_id 必须是字符串`);
			result.event_id = item.event_id.trim();
		}
		if (item.condition !== undefined) {
			if (typeof item.condition !== "string") throw new Error(`forecast.assumptions[${index}].condition 必须是字符串`);
			result.condition = item.condition.trim();
		}
		if (item.fiscal_period_end !== undefined && item.fiscal_period_end !== null) {
			const period = typeof item.fiscal_period_end === "string" ? item.fiscal_period_end.trim() : "";
			if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(period) || Number.isNaN(new Date(period).getTime())) throw new Error(`forecast.assumptions[${index}].fiscal_period_end 必须是 RFC3339 时间`);
			result.fiscal_period_end = new Date(period).toISOString();
		}
		if (typeof result.event_id === "string" && result.event_id && !result.fiscal_period_end) throw new Error(`forecast.assumptions[${index}] 关联事件时必须填写 fiscal_period_end`);
		return result;
	});
}

function validateValuation(value: unknown) {
	const valuation = objectValue(value, "valuation 必须是对象");
	rejectUnknown(valuation, ["net_debt_snapshot_id", "dcf_scenarios", "multiple_scenarios", "sensitivity_wacc", "sensitivity_terminal_growth"], "valuation");
	const dcfSource = valuation.dcf_scenarios ?? [];
	const multipleSource = valuation.multiple_scenarios ?? [];
	if (!Array.isArray(dcfSource) || !Array.isArray(multipleSource)) throw new Error("估值情景必须是数组");
	if (dcfSource.length + multipleSource.length === 0) throw new Error("至少需要一个 DCF 或估值倍数情景");
	if (dcfSource.length > 0) valuation.net_debt_snapshot_id = requiredString(valuation.net_debt_snapshot_id, "valuation.net_debt_snapshot_id");
	else if (valuation.net_debt_snapshot_id !== undefined && typeof valuation.net_debt_snapshot_id !== "string") throw new Error("valuation.net_debt_snapshot_id 必须是字符串");
	const dcfScenarios = dcfSource.map((entry, index) => {
		const item = objectValue(entry, `valuation.dcf_scenarios[${index}] 必须是对象`);
		rejectUnknown(item, ["name", "wacc", "terminal_growth", "projection_years", "net_debt", "cost_of_capital_evidence_ids"], `valuation.dcf_scenarios[${index}]`);
		const wacc = finiteNumber(item.wacc, `valuation.dcf_scenarios[${index}].wacc`);
		const growth = finiteNumber(item.terminal_growth, `valuation.dcf_scenarios[${index}].terminal_growth`);
		if (wacc <= 0 || wacc >= 1 || growth < -1 || wacc <= growth) throw new Error(`valuation.dcf_scenarios[${index}] 的 WACC/永续增长率无效`);
		return { ...item, name: requiredString(item.name, `valuation.dcf_scenarios[${index}].name`), wacc, terminal_growth: growth, projection_years: integerInRange(item.projection_years, `valuation.dcf_scenarios[${index}].projection_years`, 1, 30), net_debt: finiteNumber(item.net_debt, `valuation.dcf_scenarios[${index}].net_debt`), cost_of_capital_evidence_ids: cleanStringList(item.cost_of_capital_evidence_ids, `valuation.dcf_scenarios[${index}].cost_of_capital_evidence_ids`) };
	});
	const multipleScenarios = multipleSource.map((entry, index) => {
		const item = objectValue(entry, `valuation.multiple_scenarios[${index}] 必须是对象`);
		rejectUnknown(item, ["name", "price_earnings_multiple", "comparable_evidence_ids"], `valuation.multiple_scenarios[${index}]`);
		const multiple = finiteNumber(item.price_earnings_multiple, `valuation.multiple_scenarios[${index}].price_earnings_multiple`);
		if (multiple <= 0 || multiple > 200) throw new Error(`valuation.multiple_scenarios[${index}].price_earnings_multiple 必须大于 0 且不超过 200`);
		return { ...item, name: requiredString(item.name, `valuation.multiple_scenarios[${index}].name`), price_earnings_multiple: multiple, comparable_evidence_ids: cleanStringList(item.comparable_evidence_ids, `valuation.multiple_scenarios[${index}].comparable_evidence_ids`) };
	});
	for (const field of ["sensitivity_wacc", "sensitivity_terminal_growth"] as const) {
		if (valuation[field] === undefined) continue;
		if (!Array.isArray(valuation[field])) throw new Error(`valuation.${field} 必须是数值数组`);
		valuation[field] = (valuation[field] as unknown[]).map((item, index) => finiteNumber(item, `valuation.${field}[${index}]`));
	}
	valuation.dcf_scenarios = dcfScenarios;
	valuation.multiple_scenarios = multipleScenarios;
	return { valuation, dcfScenarios, multipleScenarios };
}

function validatePolicy(value: unknown) {
	const policy = objectValue(value, "rating.policy 必须是对象");
	rejectUnknown(policy, ["version", "market", "asset_class", "horizon_days", "benchmark_id", "relative_required", "strong_buy_floor", "buy_floor", "sell_ceiling", "strong_sell_ceiling"], "rating.policy");
	policy.version = requiredString(policy.version, "rating.policy.version");
	policy.market = requiredString(policy.market, "rating.policy.market");
	policy.asset_class = requiredString(policy.asset_class, "rating.policy.asset_class");
	policy.horizon_days = integerInRange(policy.horizon_days, "rating.policy.horizon_days", 1, 3650);
	for (const field of ["strong_buy_floor", "buy_floor", "sell_ceiling", "strong_sell_ceiling"] as const) policy[field] = finiteNumber(policy[field], `rating.policy.${field}`);
	if (typeof policy.relative_required !== "boolean") throw new Error("rating.policy.relative_required 必须是布尔值");
	if ((policy.strong_sell_ceiling as number) >= (policy.sell_ceiling as number) || (policy.sell_ceiling as number) >= (policy.buy_floor as number) || (policy.buy_floor as number) >= (policy.strong_buy_floor as number)) throw new Error("rating.policy 评级阈值顺序无效");
	if (policy.relative_required === true) policy.benchmark_id = requiredString(policy.benchmark_id, "rating.policy.benchmark_id");
	else if (policy.benchmark_id !== undefined && typeof policy.benchmark_id !== "string") throw new Error("rating.policy.benchmark_id 必须是字符串");
	return policy;
}

function validateRating(value: unknown, scheduled: boolean) {
	const rating = objectValue(value, "rating 必须是对象");
	rejectUnknown(rating, scheduled
		? ["policy", "expected_dividend", "benchmark_return", "benchmark_evidence_id", "reason_codes", "changed_assumptions", "evidence_ids", "invalidation_rules"]
		: ["policy", "as_of_price", "as_of_price_evidence_id", "expected_dividend", "benchmark_return", "benchmark_evidence_id", "reason_codes", "changed_assumptions", "evidence_ids", "invalidation_rules"], "rating");
	rating.policy = validatePolicy(rating.policy);
	rating.reason_codes = cleanStringList(rating.reason_codes, "rating.reason_codes");
	rating.evidence_ids = cleanStringList(rating.evidence_ids, "rating.evidence_ids");
	rating.changed_assumptions = objectValue(rating.changed_assumptions ?? {}, "rating.changed_assumptions 必须是对象");
	if (!scheduled) {
		const price = finiteNumber(rating.as_of_price, "rating.as_of_price");
		if (price <= 0) throw new Error("rating.as_of_price 必须大于 0");
		rating.as_of_price = price;
		rating.as_of_price_evidence_id = requiredString(rating.as_of_price_evidence_id, "rating.as_of_price_evidence_id");
	}
	if ((rating.policy as Record<string, unknown>).relative_required === true) {
		const expectedReturn = finiteNumber(rating.benchmark_return, "rating.benchmark_return");
		if (expectedReturn <= -1 || expectedReturn > 10) throw new Error("rating.benchmark_return 必须大于 -1 且不超过 10");
		rating.benchmark_return = expectedReturn;
		rating.benchmark_evidence_id = requiredString(rating.benchmark_evidence_id, "rating.benchmark_evidence_id");
	} else {
		if (rating.benchmark_return !== undefined && rating.benchmark_return !== null) rating.benchmark_return = finiteNumber(rating.benchmark_return, "rating.benchmark_return");
		if (rating.benchmark_evidence_id !== undefined && typeof rating.benchmark_evidence_id !== "string") throw new Error("rating.benchmark_evidence_id 必须是字符串");
	}
	if (rating.expected_dividend !== undefined && rating.expected_dividend !== null) rating.expected_dividend = finiteNumber(rating.expected_dividend, "rating.expected_dividend");
	const ruleSource = rating.invalidation_rules ?? [];
	if (!Array.isArray(ruleSource)) throw new Error("rating.invalidation_rules 必须是数组");
	rating.invalidation_rules = ruleSource.map((entry, index) => {
		const item = objectValue(entry, `rating.invalidation_rules[${index}] 必须是对象`);
		rejectUnknown(item, ["id", "rule_type", "operator", "threshold", "evidence_ids", "status", "metadata"], `rating.invalidation_rules[${index}]`);
		const operator = requiredString(item.operator, `rating.invalidation_rules[${index}].operator`);
		if (!invalidationOperators.has(operator)) throw new Error(`rating.invalidation_rules[${index}].operator 不受支持`);
		if (operator !== "missing" && (item.threshold === undefined || item.threshold === null || item.threshold === "")) throw new Error(`rating.invalidation_rules[${index}].threshold 不能为空`);
		for (const field of ["id", "status"] as const) if (item[field] !== undefined && typeof item[field] !== "string") throw new Error(`rating.invalidation_rules[${index}].${field} 必须是字符串`);
		if (item.metadata !== undefined) objectValue(item.metadata, `rating.invalidation_rules[${index}].metadata 必须是对象`);
		return { ...item, rule_type: requiredString(item.rule_type, `rating.invalidation_rules[${index}].rule_type`), operator, evidence_ids: cleanStringList(item.evidence_ids, `rating.invalidation_rules[${index}].evidence_ids`) };
	});
	return rating;
}

function collectEvidenceIDs(forecast: Record<string, unknown> | undefined, valuation: Record<string, unknown>, rating: Record<string, unknown>) {
	const values: string[] = [];
	if (forecast) for (const item of forecast.assumptions as Array<Record<string, unknown>>) values.push(...item.evidence_ids as string[]);
	for (const item of valuation.dcf_scenarios as Array<Record<string, unknown>>) values.push(...item.cost_of_capital_evidence_ids as string[]);
	for (const item of valuation.multiple_scenarios as Array<Record<string, unknown>>) values.push(...item.comparable_evidence_ids as string[]);
	values.push(...rating.evidence_ids as string[]);
	if (typeof rating.benchmark_evidence_id === "string" && rating.benchmark_evidence_id) values.push(rating.benchmark_evidence_id);
	for (const item of rating.invalidation_rules as Array<Record<string, unknown>>) values.push(...item.evidence_ids as string[]);
	return [...new Set(values)].sort();
}

export function fundamentalWorkflowBody(assetID: string, raw: string, now = new Date()): WorkflowPreview {
	if (Number.isNaN(now.getTime())) throw new Error("当前校验时间无效");
	const body = parseObjectJSON(raw, "基本面研究输入必须是有效 JSON 对象");
	rejectUnknown(body, ["asset_id", "as_of", "forecast", "valuation", "rating"], "基本面研究输入");
	const canonicalAssetID = requiredString(assetID, "当前资产 ID");
	if (body.asset_id !== undefined && requiredString(body.asset_id, "asset_id") !== canonicalAssetID) throw new Error("asset_id 必须与当前标的一致");
	body.asset_id = canonicalAssetID;
	const asOf = parseRFC3339(body.as_of, "as_of", now);
	body.as_of = asOf.toISOString();
	const forecast = objectValue(body.forecast, "forecast 必须是对象");
	rejectUnknown(forecast, ["parent_version_id", "inputs", "fundamental_snapshot_ids", "assumptions"], "forecast");
	if (forecast.parent_version_id !== undefined && typeof forecast.parent_version_id !== "string") throw new Error("forecast.parent_version_id 必须是字符串");
	forecast.inputs = validateFinancialInputs(forecast.inputs);
	forecast.fundamental_snapshot_ids = cleanStringList(forecast.fundamental_snapshot_ids, "forecast.fundamental_snapshot_ids");
	forecast.assumptions = validateAssumptions(forecast.assumptions ?? []);
	body.forecast = forecast;
	const valuationResult = validateValuation(body.valuation);
	body.valuation = valuationResult.valuation;
	const rating = validateRating(body.rating, false);
	body.rating = rating;
	return {
		body, asOf: asOf.toISOString(), snapshotCount: (forecast.fundamental_snapshot_ids as string[]).length,
		assumptionCount: (forecast.assumptions as unknown[]).length, dcfScenarioCount: valuationResult.dcfScenarios.length,
		multipleScenarioCount: valuationResult.multipleScenarios.length, evidenceIDs: collectEvidenceIDs(forecast, valuationResult.valuation, rating),
	};
}

export function fundamentalScheduleBody(assetID: string, raw: string): SchedulePreview {
	const body = parseObjectJSON(raw, "定时研究计划必须是有效 JSON 对象");
	rejectUnknown(body, ["asset_id", "forecast_version_id", "valuation", "rating", "cadence_hours", "max_price_age_hours", "max_plan_age_days", "approved_by"], "定时研究计划");
	const canonicalAssetID = requiredString(assetID, "当前资产 ID");
	if (body.asset_id !== undefined && requiredString(body.asset_id, "asset_id") !== canonicalAssetID) throw new Error("asset_id 必须与当前标的一致");
	body.asset_id = canonicalAssetID;
	const forecastVersionID = requiredString(body.forecast_version_id, "forecast_version_id");
	body.forecast_version_id = forecastVersionID;
	const valuationResult = validateValuation(body.valuation);
	body.valuation = valuationResult.valuation;
	const rating = validateRating(body.rating, true);
	body.rating = rating;
	const cadenceHours = integerInRange(body.cadence_hours ?? 24, "cadence_hours", 1, 720);
	const maxPriceAgeHours = integerInRange(body.max_price_age_hours ?? 120, "max_price_age_hours", 1, 336);
	const maxPlanAgeDays = integerInRange(body.max_plan_age_days ?? 90, "max_plan_age_days", 1, 365);
	body.cadence_hours = cadenceHours; body.max_price_age_hours = maxPriceAgeHours; body.max_plan_age_days = maxPlanAgeDays;
	body.approved_by = requiredString(body.approved_by, "approved_by");
	return {
		body, forecastVersionID, cadenceHours, maxPriceAgeHours, maxPlanAgeDays,
		dcfScenarioCount: valuationResult.dcfScenarios.length, multipleScenarioCount: valuationResult.multipleScenarios.length,
		evidenceIDs: collectEvidenceIDs(undefined, valuationResult.valuation, rating),
	};
}
