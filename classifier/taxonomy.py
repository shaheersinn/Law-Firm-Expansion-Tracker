"""
Taxonomy — keywords and phrases for 17 practice departments.
Phrases (multi-word) get a 2.5× boost over single keywords.
"""

DEPARTMENTS = {
    "Corporate/M&A": {
        "keywords": [
            "merger", "acquisition", "m&a", "corporate", "transaction",
            "deal", "takeover", "divestiture", "spinoff", "amalgamation",
            "securities", "shareholder", "governance", "proxy", "going-private",
        ],
        "phrases": [
            "mergers and acquisitions", "corporate governance", "hostile takeover",
            "going private transaction", "purchase agreement", "share purchase",
            "asset purchase", "management buyout", "strategic acquisition",
        ],
    },
    "Private Equity": {
        "keywords": [
            "private equity", "pe fund", "buyout", "portfolio company",
            "venture capital", "growth equity", "fund formation", "lbo",
            "carried interest", "limited partnership",
        ],
        "phrases": [
            "private equity fund", "leveraged buyout", "portfolio company",
            "fund formation", "management fee", "co-investment",
        ],
    },
    "Capital Markets": {
        "keywords": [
            "ipo", "prospectus", "underwriting", "equity offering",
            "debt financing", "bond", "debenture", "tsx", "investment grade",
            "sedar", "continuous disclosure", "osi", "exempt distribution",
        ],
        "phrases": [
            "initial public offering", "capital markets", "bought deal",
            "short form prospectus", "national instrument", "continuous disclosure",
            "equity financing", "debt capital",
        ],
    },
    "Litigation": {
        "keywords": [
            "litigation", "lawsuit", "plaintiff", "defendant", "court",
            "trial", "appeal", "arbitration", "dispute", "injunction",
            "damages", "class action", "settlement", "judgment",
        ],
        "phrases": [
            "class action", "commercial litigation", "appellate court",
            "statement of claim", "summary judgment", "contempt of court",
        ],
    },
    "Restructuring": {
        "keywords": [
            "restructuring", "insolvency", "bankruptcy", "creditor",
            "ccaa", "bia", "receivership", "monitor", "proposal",
            "debtor", "distressed", "wind-up",
        ],
        "phrases": [
            "companies creditors arrangement", "court protection",
            "creditor protection", "debt restructuring", "plan of arrangement",
            "interim receiver",
        ],
    },
    "Real Estate": {
        "keywords": [
            "real estate", "property", "land", "zoning", "development",
            "lease", "condo", "reit", "landlord", "tenant", "conveyance",
            "subdivision", "planning",
        ],
        "phrases": [
            "real estate development", "land use planning", "commercial lease",
            "purchase and sale", "real estate investment trust",
        ],
    },
    "Tax": {
        "keywords": [
            "tax", "taxation", "cra", "income tax", "gst", "hst",
            "transfer pricing", "withholding", "estate planning",
            "treaty", "audit", "reassessment",
        ],
        "phrases": [
            "income tax act", "transfer pricing", "tax planning",
            "tax dispute", "canada revenue agency", "general anti-avoidance",
        ],
    },
    "Employment": {
        "keywords": [
            "employment", "labour", "human rights", "workplace",
            "wrongful dismissal", "collective agreement", "union",
            "arbitration", "esa", "ohrc", "harassment", "pay equity",
        ],
        "phrases": [
            "employment law", "wrongful dismissal", "constructive dismissal",
            "collective bargaining", "human rights tribunal",
        ],
    },
    "IP": {
        "keywords": [
            "intellectual property", "patent", "trademark", "copyright",
            "trade secret", "licensing", "infringement", "brand",
            "cipo", "technology transfer",
        ],
        "phrases": [
            "intellectual property", "trade-mark", "patent application",
            "technology licensing", "ip portfolio",
        ],
    },
    "Data Privacy": {
        "keywords": [
            "privacy", "data protection", "cybersecurity", "pipeda",
            "ai regulation", "breach", "personal information", "gdpr",
            "opc", "data governance", "security incident", "law 25",
        ],
        "phrases": [
            "data breach", "privacy commissioner", "personal information",
            "cybersecurity incident", "data protection", "privacy law",
            "artificial intelligence regulation",
        ],
    },
    "ESG": {
        "keywords": [
            "esg", "sustainability", "climate", "environment", "emissions",
            "carbon", "indigenous", "reconciliation", "dei", "diversity",
            "social responsibility", "impact",
        ],
        "phrases": [
            "environmental social governance", "climate disclosure",
            "net zero", "indigenous rights", "diversity equity inclusion",
            "sustainable finance",
        ],
    },
    "Energy": {
        "keywords": [
            "energy", "oil", "gas", "pipeline", "electricity", "renewable",
            "solar", "wind", "nuclear", "neb", "aeso", "lng",
            "upstream", "downstream",
        ],
        "phrases": [
            "oil and gas", "energy transition", "renewable energy",
            "national energy board", "pipeline project",
        ],
    },
    "Financial Services": {
        "keywords": [
            "banking", "finance", "fintech", "osfi", "credit", "insurance",
            "derivatives", "hedge fund", "aml", "compliance", "lending",
            "payment", "crypto", "digital asset",
        ],
        "phrases": [
            "financial services", "bank act", "financial institution",
            "anti-money laundering", "digital currency", "crypto currency",
            "payment services",
        ],
    },
    "Competition": {
        "keywords": [
            "competition", "antitrust", "cartel", "merger review",
            "abuse of dominance", "competition bureau", "consent agreement",
            "deceptive marketing",
        ],
        "phrases": [
            "competition act", "merger review", "abuse of dominance",
            "competition bureau", "anti-competitive",
        ],
    },
    "Healthcare": {
        "keywords": [
            "healthcare", "pharmaceutical", "medical", "health law",
            "biotech", "clinical trial", "drug", "patented medicine",
            "health canada",
        ],
        "phrases": [
            "health law", "pharmaceutical regulatory", "clinical trial",
            "food and drug", "patented medicine",
        ],
    },
    "Immigration": {
        "keywords": [
            "immigration", "visa", "work permit", "ircc", "refugee",
            "citizenship", "permanent resident", "lmia", "express entry",
        ],
        "phrases": [
            "immigration law", "work permit", "permanent residence",
            "labour market impact assessment",
        ],
    },
    "Infrastructure": {
        "keywords": [
            "infrastructure", "p3", "ppp", "public private", "construction",
            "transit", "toll road", "airport", "port", "procurement",
        ],
        "phrases": [
            "public private partnership", "infrastructure project",
            "design build finance", "project finance",
        ],
    },
}
