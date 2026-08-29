from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from backend.app.domain import IndustryRef


@dataclass(frozen=True)
class IndustryRule:
    industry_id: str
    sector_id: str
    name_zh: str
    name_en: str
    aliases: tuple[str, ...]


SECTORS = (
    IndustryRef(industry_id="sector:energy", level=1, name_zh="能源", name_en="Energy", aliases=["石油天然气", "oil gas"]),
    IndustryRef(industry_id="sector:materials", level=1, name_zh="原材料", name_en="Materials", aliases=["材料", "基础化工"]),
    IndustryRef(industry_id="sector:industrials", level=1, name_zh="工业", name_en="Industrials", aliases=["制造业", "工业制造"]),
    IndustryRef(industry_id="sector:consumer_discretionary", level=1, name_zh="可选消费", name_en="Consumer Discretionary", aliases=["非必需消费"]),
    IndustryRef(industry_id="sector:consumer_staples", level=1, name_zh="必选消费", name_en="Consumer Staples", aliases=["日常消费"]),
    IndustryRef(industry_id="sector:health_care", level=1, name_zh="医疗健康", name_en="Health Care", aliases=["医药生物", "医疗保健"]),
    IndustryRef(industry_id="sector:financials", level=1, name_zh="金融", name_en="Financials", aliases=["金融服务"]),
    IndustryRef(industry_id="sector:information_technology", level=1, name_zh="信息技术", name_en="Information Technology", aliases=["科技", "technology"]),
    IndustryRef(industry_id="sector:communication_services", level=1, name_zh="通信服务", name_en="Communication Services", aliases=["传媒通信"]),
    IndustryRef(industry_id="sector:utilities", level=1, name_zh="公用事业", name_en="Utilities", aliases=["电力公用事业"]),
    IndustryRef(industry_id="sector:real_estate", level=1, name_zh="房地产", name_en="Real Estate", aliases=["地产"]),
    IndustryRef(industry_id="sector:digital_assets", level=1, name_zh="数字资产", name_en="Digital Assets", aliases=["加密资产", "crypto"]),
)


RULES = (
    IndustryRule("industry:oil_gas", "sector:energy", "石油与天然气", "Oil & Gas", ("oil gas", "oil & gas", "石油", "天然气", "油气")),
    IndustryRule("industry:metals_mining", "sector:materials", "金属与采矿", "Metals & Mining", ("metals mining", "mining", "有色金属", "采矿", "黄金")),
    IndustryRule("industry:chemicals", "sector:materials", "化工", "Chemicals", ("chemical", "chemicals", "化工")),
    IndustryRule("industry:aerospace_defense", "sector:industrials", "航空航天与国防", "Aerospace & Defense", ("aerospace", "defense", "航空航天", "航空工业", "国防", "卫星制造", "航天")),
    IndustryRule("industry:machinery", "sector:industrials", "机械设备", "Machinery", ("machinery", "industrial equipment", "机械", "工业设备")),
    IndustryRule("industry:transportation", "sector:industrials", "交通运输", "Transportation", ("transportation", "airlines", "railroads", "交通运输", "航空公司", "铁路")),
    IndustryRule("industry:shipping", "sector:industrials", "航运与物流", "Shipping & Logistics", ("shipping", "logistics", "航运", "物流", "港口")),
    IndustryRule("industry:automobiles", "sector:consumer_discretionary", "汽车", "Automobiles", ("automotive", "automobile", "汽车", "新能源车")),
    IndustryRule("industry:retail", "sector:consumer_discretionary", "零售", "Retail", ("retail", "零售", "电商")),
    IndustryRule("industry:food_beverage", "sector:consumer_staples", "食品饮料", "Food & Beverage", ("food", "beverage", "食品", "饮料", "白酒")),
    IndustryRule("industry:pharmaceuticals", "sector:health_care", "制药", "Pharmaceuticals", ("pharmaceutical", "drug manufacturers", "制药", "医药")),
    IndustryRule("industry:biotechnology", "sector:health_care", "生物科技", "Biotechnology", ("biotechnology", "biotech", "生物科技", "生物制品")),
    IndustryRule("industry:medical_devices", "sector:health_care", "医疗器械", "Medical Devices", ("medical devices", "medical instruments", "医疗器械")),
    IndustryRule("industry:banks", "sector:financials", "银行", "Banks", ("banks", "banking", "银行")),
    IndustryRule("industry:insurance", "sector:financials", "保险", "Insurance", ("insurance", "保险")),
    IndustryRule("industry:capital_markets", "sector:financials", "资本市场", "Capital Markets", ("capital markets", "brokerage", "证券", "券商", "投行")),
    IndustryRule("industry:semiconductors", "sector:information_technology", "半导体", "Semiconductors", ("semiconductor", "semiconductors", "半导体", "芯片")),
    IndustryRule("industry:software", "sector:information_technology", "软件", "Software", ("software", "application software", "软件", "saas")),
    IndustryRule("industry:hardware", "sector:information_technology", "硬件与设备", "Technology Hardware", ("hardware", "electronic equipment", "电脑硬件", "电子设备")),
    IndustryRule("industry:internet", "sector:communication_services", "互联网服务", "Internet Services", ("internet", "interactive media", "互联网", "网络服务")),
    IndustryRule("industry:media_entertainment", "sector:communication_services", "传媒娱乐", "Media & Entertainment", ("media", "entertainment", "传媒", "娱乐", "游戏")),
    IndustryRule("industry:telecom", "sector:communication_services", "电信", "Telecommunication", ("telecom", "telecommunication", "电信", "通信运营")),
    IndustryRule("industry:electric_utilities", "sector:utilities", "电力", "Electric Utilities", ("electric utilities", "utilities", "电力", "公用事业")),
    IndustryRule("industry:real_estate", "sector:real_estate", "房地产开发运营", "Real Estate", ("real estate", "reit", "房地产", "地产")),
    IndustryRule("industry:cryptocurrency", "sector:digital_assets", "加密货币", "Cryptocurrency", ("cryptocurrency", "crypto", "加密货币", "数字货币", "区块链")),
)


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", unicodedata.normalize("NFKC", value).casefold())


def all_industries() -> list[IndustryRef]:
    output = list(SECTORS)
    output.extend(
        IndustryRef(
            industry_id=item.industry_id,
            parent_id=item.sector_id,
            level=2,
            name_zh=item.name_zh,
            name_en=item.name_en,
            aliases=list(item.aliases),
        )
        for item in RULES
    )
    return output


def normalize_industry(raw_sector: str = "", raw_industry: str = "") -> tuple[str, str]:
    text = _normalize(f"{raw_sector} {raw_industry}")
    if not text:
        return "", ""
    for item in RULES:
        if any(_normalize(alias) in text for alias in item.aliases):
            return item.sector_id, item.industry_id
    for sector in SECTORS:
        terms = [sector.name_zh, sector.name_en, *sector.aliases]
        if any(_normalize(term) in text for term in terms):
            return sector.industry_id, ""
    return "", ""


def industries_mentioned(text: str) -> list[str]:
    normalized = _normalize(text)
    matches: list[str] = []
    for item in RULES:
        if any(_normalize(alias) in normalized for alias in item.aliases):
            matches.append(item.industry_id)
    return list(dict.fromkeys(matches))
