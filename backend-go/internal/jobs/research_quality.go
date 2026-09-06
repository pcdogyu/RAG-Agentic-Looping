package jobs

import (
	"math"
	"strings"
	"time"
)

var economicEndpointTerms = map[string][]string{
	"supply":       {"supply", "供给", "供应", "产量"},
	"demand":       {"demand", "需求", "销量"},
	"revenue":      {"revenue", "sales", "营收", "收入"},
	"cost":         {"cost", "expense", "成本", "费用"},
	"profit":       {"profit", "earnings", "margin", "利润", "盈利", "毛利"},
	"cash_flow":    {"cashflow", "cash flow", "现金流"},
	"valuation":    {"valuation", "multiple", "估值", "市盈率", "市净率"},
	"risk_premium": {"riskpremium", "risk premium", "风险溢价", "折现率"},
}

// Conditional missing information records a bounded uncertainty after the
// event-to-target relation and transmission evidence have passed. Every other
// missing item remains critical by default, so a model cannot downgrade a
// missing issuer, citation, unit, period or transmission link into a harmless
// scenario merely by adding a prefix.
func conditionalMissingInformation(values []string) []string {
	result := []string{}
	for _, value := range values {
		raw := strings.TrimSpace(value)
		lower := strings.ToLower(raw)
		if !strings.HasPrefix(lower, "conditional:") {
			continue
		}
		detail := strings.TrimSpace(raw[len("conditional:"):])
		if detail == "" || criticalMissingDetail(detail) {
			continue
		}
		result = append(result, detail)
	}
	return uniqueStrings(result)
}

func criticalMissingInformation(values []string) []string {
	result := []string{}
	for _, value := range values {
		raw := strings.TrimSpace(value)
		if raw == "" {
			continue
		}
		lower := strings.ToLower(raw)
		if !strings.HasPrefix(lower, "conditional:") {
			result = append(result, raw)
			continue
		}
		detail := strings.TrimSpace(raw[len("conditional:"):])
		if detail == "" || criticalMissingDetail(detail) {
			result = append(result, fallbackString(detail, raw))
		}
	}
	return uniqueStrings(result)
}

func criticalMissingDetail(value string) bool {
	value = strings.ToLower(value)
	for _, marker := range []string{
		"target", "issuer", "security", "relation", "evidence", "citation", "action", "transmission", "endpoint", "economic",
		"currency", "unit", "period", "scope", "标的", "发行", "证券", "关系", "证据", "引用", "动作", "传导", "终点", "币种", "单位", "期间", "口径", "范围",
	} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func appendModelMissing(verification *draftVerification, values []string) {
	verification.Missing = append(verification.Missing, criticalMissingInformation(values)...)
	verification.Conditional = append(verification.Conditional, conditionalMissingInformation(values)...)
}

func verifyEventDraft(draft *eventResearchDraft, event map[string]any, evidence []researchEvidence, asOf time.Time) draftVerification {
	verification := draftVerification{StructurallyValid: true}
	if draft == nil {
		return draftVerification{Missing: []string{"draft"}}
	}
	if strings.TrimSpace(draft.Summary) == "" {
		verification.StructurallyValid = false
		verification.Missing = append(verification.Missing, "summary")
	}
	if !asOf.IsZero() {
		for _, item := range evidence {
			if item.PublishedAt.After(asOf) || item.ObservedAt.After(asOf) || item.AsOf.After(asOf) {
				verification.Contradictions = append(verification.Contradictions, "point-in-time boundary violation")
				break
			}
		}
	}
	globalContradiction := len(verification.Contradictions) > 0

	allowed := candidateAssets(event)
	validEvidence := stringSet(evidenceIDs(evidence))
	validActions := eventActionIDs(event)
	filter := objectValue(event["recent_research_filter"])
	excludedAssets := stringSlice(filter["excluded_asset_terms"])
	excludedIndustries := stringSlice(filter["excluded_industry_terms"])
	seen := map[string]bool{}
	validImpacts := make([]eventImpactDraft, 0, min(6, len(draft.Impacts)))
	allEvidence := []string{}
	allComplete := true

	for _, original := range draft.Impacts {
		item := original
		if (item.AssetID == "" && matchesIdentityTerms(item.TargetName, excludedAssets)) || (item.TargetType == "sector" && matchesIdentityTerms(item.TargetName, excludedIndustries)) {
			verification.Missing = append(verification.Missing, "excluded_target:"+item.TargetName)
			allComplete = false
			continue
		}
		if item.AssetID == "" {
			if asset := matchCandidateAsset(item.TargetName, allowed); asset != nil {
				item.AssetID = stringValue(asset["asset_id"])
				item.TargetType, item.TargetName = "tradable_asset", stringValue(asset["name"])
			}
		}
		if item.TargetType == "tradable_asset" && item.AssetID == "" {
			verification.Missing = append(verification.Missing, "unknown allowed target: "+item.TargetName)
			allComplete = false
			continue
		}
		if item.AssetID != "" {
			asset := allowed[item.AssetID]
			if asset == nil {
				asset = matchCandidateAsset(item.AssetID, allowed)
			}
			if asset == nil {
				asset = matchCandidateAsset(item.TargetName, allowed)
			}
			if asset == nil {
				verification.Missing = append(verification.Missing, "unknown allowed target: "+item.AssetID)
				allComplete = false
				continue
			}
			item.AssetID = stringValue(asset["asset_id"])
			item.TargetType, item.TargetName = "tradable_asset", stringValue(asset["name"])
		}
		if strings.TrimSpace(item.TargetName) == "" || nonTargetActivity(item.TargetName) {
			verification.Missing = append(verification.Missing, "invalid_target:"+item.TargetName)
			allComplete = false
			continue
		}
		key := item.TargetType + ":" + strings.ToLower(fallbackString(item.AssetID, normalizedText(item.TargetName)))
		if seen[key] {
			verification.Missing = append(verification.Missing, "duplicate_target:"+key)
			allComplete = false
			continue
		}
		seen[key] = true

		item.DirectionScore = clampInt(item.DirectionScore, -100, 100)
		if !containsString(conclusionStatuses(), item.ConclusionStatus) {
			verification.StructurallyValid = false
			verification.Missing = append(verification.Missing, "invalid_conclusion_status:"+key)
			delete(seen, key)
			allComplete = false
			continue
		}
		if !containsString(impactChannels(), item.ImpactChannel) {
			verification.StructurallyValid = false
			verification.Missing = append(verification.Missing, "invalid_impact_channel:"+key)
			delete(seen, key)
			allComplete = false
			continue
		}

		impactComplete := true
		directionComplete := len(criticalMissingInformation(item.Missing)) == 0
		item.EvidenceIDs, verification.Missing = filterReferenceIDs(item.EvidenceIDs, validEvidence, "evidence", verification.Missing)
		if item.ActionID != "" && !validActions[item.ActionID] {
			verification.Missing = append(verification.Missing, "unknown action id: "+item.ActionID)
			item.ActionID = ""
			impactComplete = false
			directionComplete = false
		}
		if item.TargetRelation.Kind != "direct" && item.TargetRelation.Kind != "indirect" {
			verification.Missing = append(verification.Missing, "target_relation.kind:"+key)
			impactComplete, directionComplete = false, false
		}
		item.TargetRelation.EvidenceIDs, verification.Missing = filterReferenceIDs(item.TargetRelation.EvidenceIDs, validEvidence, "evidence", verification.Missing)
		item.TargetRelation.ActionIDs, verification.Missing = filterReferenceIDs(item.TargetRelation.ActionIDs, validActions, "action", verification.Missing)
		item.Missing = append(item.Missing, item.TargetRelation.MissingInformation...)
		// Target relation uncertainty is always a hard gate: it establishes the
		// first event-to-issuer link and cannot be merely scenario-labelled.
		verification.Missing = append(verification.Missing, item.TargetRelation.MissingInformation...)
		if strings.TrimSpace(item.TargetRelation.Subject) == "" || len(item.TargetRelation.EvidenceIDs)+len(item.TargetRelation.ActionIDs) == 0 {
			verification.Missing = append(verification.Missing, "target_relation.evidence:"+key)
			impactComplete, directionComplete = false, false
		}
		for index := range item.Claims {
			claim := &item.Claims[index]
			if strings.TrimSpace(claim.Text) == "" || (claim.ClaimType != "fact" && claim.ClaimType != "inference") {
				verification.StructurallyValid = false
				verification.Missing = append(verification.Missing, "invalid_claim:"+key)
				impactComplete = false
				directionComplete = false
			}
			before := len(verification.Missing)
			claim.EvidenceIDs, verification.Missing = filterReferenceIDs(claim.EvidenceIDs, validEvidence, "evidence", verification.Missing)
			claim.ActionIDs, verification.Missing = filterReferenceIDs(claim.ActionIDs, validActions, "action", verification.Missing)
			if before != len(verification.Missing) || len(claim.EvidenceIDs)+len(claim.ActionIDs) == 0 {
				impactComplete = false
				directionComplete = false
			}
			if len(criticalMissingInformation(claim.MissingInformation)) > 0 {
				directionComplete = false
			}
			appendModelMissing(&verification, claim.MissingInformation)
			item.EvidenceIDs = append(item.EvidenceIDs, claim.EvidenceIDs...)
			item.Missing = append(item.Missing, claim.MissingInformation...)
		}
		if len(item.Claims) == 0 {
			verification.StructurallyValid = false
			verification.Missing = append(verification.Missing, "claims:"+key)
			impactComplete = false
			directionComplete = false
		}
		for index := range item.TransmissionSteps {
			step := &item.TransmissionSteps[index]
			if strings.TrimSpace(step.SourceNode) == "" || strings.TrimSpace(step.Mechanism) == "" || strings.TrimSpace(step.TargetNode) == "" || (step.BasisType != "fact" && step.BasisType != "inference") {
				verification.StructurallyValid = false
				verification.Missing = append(verification.Missing, "invalid_transmission_step:"+key)
				impactComplete = false
				directionComplete = false
			}
			before := len(verification.Missing)
			step.EvidenceIDs, verification.Missing = filterReferenceIDs(step.EvidenceIDs, validEvidence, "evidence", verification.Missing)
			step.ActionIDs, verification.Missing = filterReferenceIDs(step.ActionIDs, validActions, "action", verification.Missing)
			if before != len(verification.Missing) || len(step.EvidenceIDs)+len(step.ActionIDs) == 0 {
				impactComplete = false
				directionComplete = false
			}
			if len(criticalMissingInformation(step.MissingInformation)) > 0 {
				directionComplete = false
			}
			appendModelMissing(&verification, step.MissingInformation)
			item.EvidenceIDs = append(item.EvidenceIDs, step.EvidenceIDs...)
			item.Missing = append(item.Missing, step.MissingInformation...)
		}
		if len(item.TransmissionSteps) == 0 || len(item.TransmissionSteps) > 3 {
			verification.Missing = append(verification.Missing, "transmission_steps:"+key)
			impactComplete = false
			directionComplete = false
			if len(item.TransmissionSteps) > 3 {
				verification.StructurallyValid = false
				item.TransmissionSteps = item.TransmissionSteps[:3]
			}
		}
		if len(item.TransmissionPath) < 2 || len(item.TransmissionPath) > 4 {
			verification.StructurallyValid = false
			verification.Missing = append(verification.Missing, "transmission_path:"+key)
			impactComplete = false
			directionComplete = false
			if len(item.TransmissionPath) > 4 {
				item.TransmissionPath = item.TransmissionPath[:4]
			}
		}

		for _, named := range assessmentReferences(&item.TargetEvaluation) {
			name, assessment := named.Name, named.Value
			if assessment.Score < 0 || assessment.Score > 100 || strings.TrimSpace(assessment.Reason) == "" {
				verification.StructurallyValid = false
				verification.Missing = append(verification.Missing, "target_evaluation."+name+":"+key)
				impactComplete = false
				directionComplete = false
			}
			validAssessmentEvidence, assessmentMissing := filterReferenceIDs(assessment.EvidenceIDs, validEvidence, "evidence", nil)
			validAssessmentActions, actionMissing := filterReferenceIDs(assessment.ActionIDs, validActions, "action", nil)
			assessment.EvidenceIDs, assessment.ActionIDs = validAssessmentEvidence, validAssessmentActions
			verification.Missing = append(verification.Missing, assessmentMissing...)
			verification.Missing = append(verification.Missing, actionMissing...)
			if len(validAssessmentEvidence)+len(validAssessmentActions) == 0 && assessment.Score != 0 {
				verification.Missing = append(verification.Missing, "unsupported_target_evaluation."+name+":"+key)
				impactComplete = false
				directionComplete = false
			}
			item.EvidenceIDs = append(item.EvidenceIDs, validAssessmentEvidence...)
			item.Missing = append(item.Missing, assessment.MissingInformation...)
			appendModelMissing(&verification, assessment.MissingInformation)
		}

		item.EvidenceIDs = uniqueStrings(item.EvidenceIDs)
		item.Missing = uniqueStrings(item.Missing)
		impactEvidenceIDs, impactActionIDs := impactReferenceIDs(item)
		if len(impactEvidenceIDs)+len(impactActionIDs) == 0 {
			verification.Missing = append(verification.Missing, "impact_evidence:"+key)
			impactComplete = false
			directionComplete = false
		}
		if critical := criticalMissingInformation(item.Missing); len(critical) > 0 {
			impactComplete = false
			directionComplete = false
			verification.Missing = append(verification.Missing, critical...)
		}
		verification.Conditional = append(verification.Conditional, conditionalMissingInformation(item.Missing)...)
		hasEndpoint := impactHasEconomicEndpoint(item)
		pathContinuous := transmissionPathContinuous(item)
		if !pathContinuous {
			verification.Missing = append(verification.Missing, "broken_transmission_path:"+key)
			impactComplete, directionComplete = false, false
		}
		if !hasEndpoint {
			verification.Missing = append(verification.Missing, "economic_endpoint:"+key)
			impactComplete = false
		}
		if !impactHasTargetSpecificEvidence(item, event, evidence) {
			verification.Missing = append(verification.Missing, "target_specific_evidence:"+key)
			impactComplete, directionComplete = false, false
		}
		if item.ConclusionStatus == "insufficient_evidence" && item.DirectionScore != 0 {
			verification.Contradictions = append(verification.Contradictions, "insufficient conclusion had nonzero direction:"+key)
			item.DirectionScore = 0
		}
		if item.DirectionScore != 0 && (!directionComplete || !hasEndpoint || !pathContinuous || globalContradiction || item.ConclusionStatus != "directional") {
			verification.Contradictions = append(verification.Contradictions, "nonzero direction without complete support:"+key)
			item.DirectionScore = 0
			item.ConclusionStatus = "insufficient_evidence"
		}
		if item.ConclusionStatus == "neutral_supported" && item.DirectionScore != 0 {
			verification.Contradictions = append(verification.Contradictions, "neutral conclusion had nonzero direction:"+key)
			item.DirectionScore = 0
		}
		if item.ConclusionStatus == "directional" && item.DirectionScore == 0 {
			verification.Contradictions = append(verification.Contradictions, "directional conclusion had zero direction:"+key)
			item.ConclusionStatus = "insufficient_evidence"
		}
		allComplete = allComplete && impactComplete
		allEvidence = append(allEvidence, item.EvidenceIDs...)
		validImpacts = append(validImpacts, item)
		if len(validImpacts) == 6 {
			break
		}
	}

	draft.Impacts = validImpacts
	draft.EvidenceIDs = uniqueStrings(allEvidence)
	if len(validImpacts) == 0 {
		if !containsString(draft.MissingInformation, "no_confirmed_target") {
			draft.MissingInformation = append(draft.MissingInformation, "no_confirmed_target")
		}
		appendModelMissing(&verification, draft.MissingInformation)
		verification.Missing = uniqueStrings(verification.Missing)
		verification.Conditional = uniqueStrings(verification.Conditional)
		verification.Contradictions = uniqueStrings(verification.Contradictions)
		return verification
	}
	appendModelMissing(&verification, draft.MissingInformation)
	official, groups := citedSourceCoverage(draft.EvidenceIDs, evidence)
	if !official && groups < 2 {
		verification.Missing = append(verification.Missing, "one official source or two independent sources")
		allComplete = false
	}
	verification.Missing = uniqueStrings(verification.Missing)
	verification.Conditional = uniqueStrings(verification.Conditional)
	verification.Contradictions = uniqueStrings(verification.Contradictions)
	verification.EvidenceComplete = verification.StructurallyValid && allComplete && len(verification.Missing) == 0 && len(verification.Contradictions) == 0
	return verification
}

func verifyAssetDraft(draft *assetResearchDraft, asset, event map[string]any, evidence []researchEvidence, asOf time.Time) draftVerification {
	if draft == nil {
		return draftVerification{Missing: []string{"draft"}}
	}
	eventCopy := cloneMap(event)
	if eventCopy == nil {
		eventCopy = map[string]any{}
	}
	assetID := stringValue(asset["asset_id"])
	if candidateAssets(eventCopy)[assetID] == nil {
		eventCopy["candidates"] = append([]any{map[string]any{"asset": asset, "relationship": "direct", "relevance": 1.0, "mapping_confidence": 1.0}}, anySlice(eventCopy["candidates"])...)
	}
	eventDraft := eventResearchDraft{
		Summary: draft.Summary, EvidenceIDs: draft.EvidenceIDs, MissingInformation: draft.MissingInformation,
		Impacts: []eventImpactDraft{eventImpactFromAssetDraft(*draft, asset)},
	}
	verification := verifyEventDraft(&eventDraft, eventCopy, evidence, asOf)
	if len(eventDraft.Impacts) == 1 {
		impact := eventDraft.Impacts[0]
		draft.DirectionScore, draft.ConclusionStatus, draft.ImpactChannel = impact.DirectionScore, impact.ConclusionStatus, impact.ImpactChannel
		draft.Claims, draft.TransmissionSteps, draft.TransmissionPath, draft.TargetRelation = impact.Claims, impact.TransmissionSteps, impact.TransmissionPath, impact.TargetRelation
		draft.EvidenceIDs, draft.MissingInformation = impact.EvidenceIDs, impact.Missing
	} else {
		draft.DirectionScore, draft.ConclusionStatus, draft.EvidenceIDs = 0, "insufficient_evidence", []string{}
	}
	return verification
}

func eventImpactFromAssetDraft(draft assetResearchDraft, asset map[string]any) eventImpactDraft {
	return eventImpactDraft{
		TargetType: "tradable_asset", TargetName: stringValue(asset["name"]), AssetID: stringValue(asset["asset_id"]),
		ConclusionStatus: draft.ConclusionStatus, ImpactChannel: draft.ImpactChannel, DirectionScore: draft.DirectionScore,
		Claims: draft.Claims, TransmissionSteps: draft.TransmissionSteps, TransmissionPath: draft.TransmissionPath, TargetRelation: draft.TargetRelation,
		TargetEvaluation: draft.TargetEvaluation, Rationale: draft.Summary, EvidenceIDs: draft.EvidenceIDs, Missing: draft.MissingInformation,
	}
}

type namedAssessment struct {
	Name  string
	Value evidenceAssessmentDraft
}

type namedAssessmentReference struct {
	Name  string
	Value *evidenceAssessmentDraft
}

func assessmentReferences(value *targetEvaluationDraft) []namedAssessmentReference {
	return []namedAssessmentReference{
		{Name: "object_relevance", Value: &value.ObjectRelevance},
		{Name: "evidence_sufficiency", Value: &value.EvidenceSufficiency},
		{Name: "transmission_certainty", Value: &value.TransmissionCertainty},
		{Name: "impact_support", Value: &value.ImpactSupport},
		{Name: "timing_persistence", Value: &value.TimingPersistence},
	}
}

func namedAssessments(value targetEvaluationDraft) []namedAssessment {
	return []namedAssessment{
		{Name: "object_relevance", Value: value.ObjectRelevance},
		{Name: "evidence_sufficiency", Value: value.EvidenceSufficiency},
		{Name: "transmission_certainty", Value: value.TransmissionCertainty},
		{Name: "impact_support", Value: value.ImpactSupport},
		{Name: "timing_persistence", Value: value.TimingPersistence},
	}
}

func filterReferenceIDs(values []string, valid map[string]bool, kind string, missing []string) ([]string, []string) {
	result := []string{}
	for _, value := range uniqueStrings(values) {
		if valid[value] {
			result = append(result, value)
		} else {
			missing = append(missing, "unknown "+kind+" id: "+value)
		}
	}
	return result, missing
}

func eventActionIDs(event map[string]any) map[string]bool {
	result := map[string]bool{}
	for _, raw := range anySlice(event["actions"]) {
		if id := stringValue(objectValue(raw)["id"]); id != "" {
			result[id] = true
		}
	}
	return result
}

func impactReferenceIDs(item eventImpactDraft) ([]string, []string) {
	evidenceIDs, actionIDs := append([]string{}, item.EvidenceIDs...), []string{}
	if item.ActionID != "" {
		actionIDs = append(actionIDs, item.ActionID)
	}
	for _, claim := range item.Claims {
		evidenceIDs, actionIDs = append(evidenceIDs, claim.EvidenceIDs...), append(actionIDs, claim.ActionIDs...)
	}
	for _, step := range item.TransmissionSteps {
		evidenceIDs, actionIDs = append(evidenceIDs, step.EvidenceIDs...), append(actionIDs, step.ActionIDs...)
	}
	evidenceIDs, actionIDs = append(evidenceIDs, item.TargetRelation.EvidenceIDs...), append(actionIDs, item.TargetRelation.ActionIDs...)
	for _, assessment := range namedAssessments(item.TargetEvaluation) {
		evidenceIDs, actionIDs = append(evidenceIDs, assessment.Value.EvidenceIDs...), append(actionIDs, assessment.Value.ActionIDs...)
	}
	return uniqueStrings(evidenceIDs), uniqueStrings(actionIDs)
}

func impactHasTargetSpecificEvidence(item eventImpactDraft, event map[string]any, evidence []researchEvidence) bool {
	relation := item.TargetRelation
	if (relation.Kind != "direct" && relation.Kind != "indirect") || strings.TrimSpace(relation.Subject) == "" || len(relation.MissingInformation) > 0 {
		return false
	}
	if relation.Kind == "indirect" && !containsString([]string{"supplier", "customer", "competitor", "holder", "business_exposure"}, relation.RelationshipType) {
		return false
	}
	if relation.Kind == "direct" && !containsString([]string{"issuer", "security_identifier"}, relation.RelationshipType) {
		return false
	}
	evidenceIDs, actionIDs := relation.EvidenceIDs, relation.ActionIDs
	if len(evidenceIDs)+len(actionIDs) == 0 {
		return false
	}
	asset := candidateAssets(event)[item.AssetID]
	identity := targetIdentityTerms(item.TargetName, asset)
	if len(identity) == 0 {
		return false
	}
	allowedEvidence, allowedActions := stringSet(evidenceIDs), stringSet(actionIDs)
	mentioned, currentEventSupport := false, false
	for _, current := range evidence {
		if allowedEvidence[current.ID] && containsTargetIdentity(current.Claim+" "+current.Excerpt, identity) {
			mentioned = true
			currentEventSupport = current.ContextRole != "historical_context"
			break
		}
	}
	if !mentioned {
		for _, raw := range anySlice(event["actions"]) {
			action := objectValue(raw)
			if allowedActions[stringValue(action["id"])] && containsTargetIdentity(stringValue(action["actor"])+" "+stringValue(action["object"])+" "+stringValue(action["scope"]), identity) {
				mentioned = true
				currentEventSupport = true
				break
			}
		}
	}
	if !mentioned || !currentEventSupport {
		return false
	}
	if item.TargetType == "tradable_asset" {
		return asset != nil
	}
	target := normalizedText(item.TargetName)
	if target == "" {
		return false
	}
	allowedEvidence = stringSet(evidenceIDs)
	for _, current := range evidence {
		if allowedEvidence[current.ID] && strings.Contains(normalizedText(current.Claim+" "+current.Excerpt), target) {
			return true
		}
	}
	allowedActions = stringSet(actionIDs)
	for _, raw := range anySlice(event["actions"]) {
		action := objectValue(raw)
		if allowedActions[stringValue(action["id"])] && strings.Contains(normalizedText(stringValue(action["actor"])+" "+stringValue(action["object"])+" "+stringValue(action["scope"])), target) {
			return true
		}
	}
	return false
}

func targetIdentityTerms(target string, asset map[string]any) []string {
	values := []string{target}
	if asset != nil {
		values = append(values, stringValue(asset["name"]), stringValue(asset["symbol"]))
		values = append(values, stringSlice(asset["aliases"])...)
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if len([]rune(value)) >= 2 && !containsString(result, value) {
			result = append(result, value)
		}
	}
	return result
}

func containsTargetIdentity(value string, terms []string) bool {
	normalized := normalizedText(value)
	for _, term := range terms {
		needle := normalizedText(term)
		if needle != "" && strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

func impactHasEconomicEndpoint(item eventImpactDraft) bool {
	terms, valid := economicEndpointTerms[item.ImpactChannel]
	if !valid || len(item.TransmissionSteps) == 0 || len(item.TransmissionPath) < 2 {
		return false
	}
	if item.TargetType == "tradable_asset" && (item.ImpactChannel == "supply" || item.ImpactChannel == "demand") {
		return false
	}
	last := item.TransmissionSteps[len(item.TransmissionSteps)-1].TargetNode + " " + item.TransmissionPath[len(item.TransmissionPath)-1]
	last = strings.ToLower(last)
	for _, term := range terms {
		if strings.Contains(last, strings.ToLower(term)) || strings.Contains(normalizedText(last), normalizedText(term)) {
			return true
		}
	}
	return false
}

func transmissionPathContinuous(item eventImpactDraft) bool {
	if len(item.TransmissionSteps) == 0 || len(item.TransmissionPath) != len(item.TransmissionSteps)+1 {
		return false
	}
	for index, step := range item.TransmissionSteps {
		if normalizedText(step.SourceNode) != normalizedText(item.TransmissionPath[index]) || normalizedText(step.TargetNode) != normalizedText(item.TransmissionPath[index+1]) {
			return false
		}
	}
	return true
}

func citedSourceCoverage(ids []string, evidence []researchEvidence) (bool, int) {
	set := stringSet(ids)
	official, groups := false, map[string]bool{}
	for _, item := range evidence {
		if !set[item.ID] {
			continue
		}
		official = official || item.SourceQuality == "official"
		if item.IndependentGroup != "" {
			groups[item.IndependentGroup] = true
		}
	}
	return official, len(groups)
}

func finalizeTargetEvaluation(item eventImpactDraft, event map[string]any, evidence []researchEvidence, contradictions []string) targetEvaluationDraft {
	result := item.TargetEvaluation
	validEvidence, validActions := stringSet(evidenceIDs(evidence)), eventActionIDs(event)
	apply := func(value evidenceAssessmentDraft) evidenceAssessmentDraft {
		value.Score = clampInt(value.Score, 0, 100)
		value.EvidenceIDs, _ = filterReferenceIDs(value.EvidenceIDs, validEvidence, "evidence", nil)
		value.ActionIDs, _ = filterReferenceIDs(value.ActionIDs, validActions, "action", nil)
		value.MissingInformation = uniqueStrings(value.MissingInformation)
		value.CapReasons = []string{}
		if len(value.EvidenceIDs)+len(value.ActionIDs) == 0 {
			capAssessment(&value, 0, "no_valid_support_id")
		}
		return value
	}
	result.ObjectRelevance = apply(result.ObjectRelevance)
	result.EvidenceSufficiency = apply(result.EvidenceSufficiency)
	result.TransmissionCertainty = apply(result.TransmissionCertainty)
	result.ImpactSupport = apply(result.ImpactSupport)
	result.TimingPersistence = apply(result.TimingPersistence)

	if !impactHasTargetSpecificEvidence(item, event, evidence) {
		capAssessment(&result.EvidenceSufficiency, 0, "no_target_specific_evidence")
	}
	impactEvidence, _ := impactReferenceIDs(item)
	if official, groups := citedSourceCoverage(impactEvidence, evidence); !official && groups < 2 {
		capAssessment(&result.EvidenceSufficiency, 49, "source_independence_gate")
	}
	if len(item.TransmissionSteps) == 0 {
		capAssessment(&result.TransmissionCertainty, 0, "missing_transmission_steps")
		capAssessment(&result.ImpactSupport, 39, "missing_transmission_steps")
	} else if transmissionHasMissingInformation(item.TransmissionSteps) {
		capAssessment(&result.TransmissionCertainty, 39, "incomplete_transmission_step")
	}
	if !impactHasEconomicEndpoint(item) {
		capAssessment(&result.TransmissionCertainty, 39, "missing_economic_endpoint")
		capAssessment(&result.ImpactSupport, 39, "missing_economic_endpoint")
	}
	if impactTimingUnknown(item, event) {
		capAssessment(&result.TimingPersistence, 20, "unknown_action_stage")
	}
	if item.ConclusionStatus == "insufficient_evidence" {
		capAssessment(&result.ImpactSupport, 39, "insufficient_evidence")
	}
	if len(contradictions) > 0 {
		capAssessment(&result.EvidenceSufficiency, 39, "unresolved_contradiction")
		capAssessment(&result.TransmissionCertainty, 39, "unresolved_contradiction")
		capAssessment(&result.ImpactSupport, 39, "unresolved_contradiction")
		capAssessment(&result.TimingPersistence, 39, "unresolved_contradiction")
	}
	return result
}

func transmissionHasMissingInformation(steps []transmissionStepDraft) bool {
	for _, step := range steps {
		if len(criticalMissingInformation(step.MissingInformation)) > 0 {
			return true
		}
	}
	return false
}

func capAssessment(value *evidenceAssessmentDraft, maximum int, reason string) {
	if value.Score > maximum {
		value.Score = maximum
	}
	if value.Score == maximum {
		value.CapReasons = appendUnique(value.CapReasons, reason)
	}
}

func impactTimingUnknown(item eventImpactDraft, event map[string]any) bool {
	_, actionIDs := impactReferenceIDs(item)
	if len(actionIDs) == 0 {
		return true
	}
	set := stringSet(actionIDs)
	for _, raw := range anySlice(event["actions"]) {
		action := objectValue(raw)
		if !set[stringValue(action["id"])] {
			continue
		}
		stage := stringValue(action["action_stage"])
		if stage == "" || stage == "unknown" {
			return true
		}
	}
	return false
}

func targetEvaluationScore(value targetEvaluationDraft) int {
	return clampInt(int(math.Round(.20*float64(value.ObjectRelevance.Score)+.25*float64(value.EvidenceSufficiency.Score)+.25*float64(value.TransmissionCertainty.Score)+.20*float64(value.ImpactSupport.Score)+.10*float64(value.TimingPersistence.Score))), 0, 100)
}

func targetEvaluationCapReasons(value targetEvaluationDraft) []string {
	result := []string{}
	for _, assessment := range namedAssessments(value) {
		result = append(result, assessment.Value.CapReasons...)
	}
	return uniqueStrings(result)
}

func reportConfidenceScore(newsConfidence float64, targetScores []int, verification draftVerification) float64 {
	if len(targetScores) == 0 {
		return 0
	}
	total := 0
	for _, score := range targetScores {
		total += score
	}
	value := math.Min(newsConfidence, float64(total)/float64(len(targetScores))/100)
	if !verification.EvidenceComplete {
		value = math.Min(value, .49)
	}
	if len(verification.Contradictions) > 0 {
		value = math.Min(value, .39)
	}
	return round4(value)
}

func nonNilClaims(values []claimDraft) []claimDraft {
	if values == nil {
		return []claimDraft{}
	}
	return values
}

func nonNilTransmissionSteps(values []transmissionStepDraft) []transmissionStepDraft {
	if values == nil {
		return []transmissionStepDraft{}
	}
	return values
}
