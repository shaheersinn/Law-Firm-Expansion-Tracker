"""
Practice department taxonomy.
Each department has:
  - keywords  : single-word triggers (weight 1.0×)
  - phrases   : multi-word triggers (weight 2.5×)
  - base_weight: signal weight multiplier for that department
"""

DEPARTMENTS: dict[str, dict] = {
    "Corporate/M&A": {
        "base_weight": 1.4,
        "keywords": [
            "mergers", "acquisitions", "m&a", "corporate", "transaction",
            "takeover", "divestiture", "privatization", "joint venture",
            "shareholder", "amalgamation", "corporate finance", "buyout",
        ],
        "phrases": [
            "mergers and acquisitions", "corporate transactions",
            "cross-border m&a", "strategic acquisitions", "hostile takeover",
            "corporate advisory", "deal counsel", "acquisition financing",
        ],
    },
    "Private Equity": {
        "base_weight": 1.5,
        "keywords": [
            "private equity", "pe", "fund", "venture capital", "buyout",
            "portfolio", "lbo", "leveraged", "fund formation", "gp", "lp",
        ],
        "phrases": [
            "private equity fund", "venture capital fund", "fund formation",
            "leveraged buyout", "management buyout", "growth equity",
            "private credit", "limited partnership",
        ],
    },
    "Capital Markets": {
        "base_weight": 1.4,
        "keywords": [
            "securities", "ipo", "prospectus", "equity offering", "debt",
            "underwriting", "tsx", "toronto stock exchange", "sedar",
            "continuous disclosure", "capital markets", "offering",
        ],
        "phrases": [
            "initial public offering", "bought deal", "capital markets",
            "securities regulation", "continuous disclosure obligations",
            "prospectus offering", "debt capital markets",
        ],
    },
    "Litigation": {
        "base_weight": 1.3,
        "keywords": [
            "litigation", "dispute", "arbitration", "mediating", "trial",
            "appeal", "injunction", "class action", "plaintiff", "defendant",
            "court", "commercial litigation", "judgment", "settlement",
        ],
        "phrases": [
            "commercial litigation", "class action", "appellate advocacy",
            "international arbitration", "dispute resolution",
            "injunctive relief", "securities litigation",
        ],
    },
    "Restructuring": {
        "base_weight": 1.5,
        "keywords": [
            "restructuring", "insolvency", "bankruptcy", "ccaa", "bia",
            "creditor", "debtor", "monitor", "receiver", "receivership",
            "wind-up", "liquidation", "turnaround",
        ],
        "phrases": [
            "companies creditors arrangement act", "creditor protection",
            "debt restructuring", "insolvency proceedings",
            "court-supervised restructuring", "creditor rights",
            "distressed assets",
        ],
    },
    "Real Estate": {
        "base_weight": 1.3,
        "keywords": [
            "real estate", "property", "leasing", "zoning", "development",
            "reit", "land", "commercial property", "residential", "condo",
            "strata", "title", "mortgage", "condominium",
        ],
        "phrases": [
            "real estate development", "commercial leasing",
            "land development", "real estate investment trust",
            "property acquisition", "real estate finance",
            "commercial real estate",
        ],
    },
    "Tax": {
        "base_weight": 1.4,
        "keywords": [
            "tax", "taxation", "gst", "hst", "income tax", "transfer pricing",
            "cra", "audit", "treaty", "withholding", "excise", "customs",
        ],
        "phrases": [
            "income tax", "transfer pricing", "tax planning",
            "tax controversy", "indirect tax", "international tax",
            "tax structuring", "goods and services tax",
        ],
    },
    "Employment": {
        "base_weight": 1.6,
        "keywords": [
            "employment", "labour", "labor", "human rights", "termination",
            "wrongful dismissal", "collective bargaining", "union",
            "workplace", "discrimination", "harassment", "hiring",
            "employment standards",
        ],
        "phrases": [
            "employment law", "labour relations", "wrongful dismissal",
            "employment standards act", "human rights", "collective agreement",
            "employment rights act", "workplace harassment",
        ],
    },
    "IP": {
        "base_weight": 1.4,
        "keywords": [
            "intellectual property", "patent", "trademark", "copyright",
            "cipo", "trade secret", "licensing", "ip", "infringement",
            "brand", "innovation", "design",
        ],
        "phrases": [
            "intellectual property", "patent prosecution", "trademark registration",
            "copyright infringement", "trade secret", "ip licensing",
            "technology transfer", "brand protection",
        ],
    },
    "Data Privacy": {
        "base_weight": 1.8,
        "keywords": [
            "privacy", "cybersecurity", "data breach", "pipeda", "casl",
            "gdpr", "ai", "artificial intelligence", "data protection",
            "surveillance", "biometric", "cloud", "data governance",
        ],
        "phrases": [
            "data privacy", "cybersecurity", "data breach", "personal information",
            "privacy commissioner", "artificial intelligence regulation",
            "data protection law", "privacy compliance",
            "ai regulation", "digital economy",
        ],
    },
    "ESG": {
        "base_weight": 1.5,
        "keywords": [
            "esg", "environmental", "sustainability", "climate", "carbon",
            "emissions", "net zero", "indigenous", "reconciliation",
            "social governance", "impact investing", "green",
        ],
        "phrases": [
            "esg", "environmental social governance", "climate change",
            "indigenous reconciliation", "clean energy",
            "carbon tax", "net-zero", "sustainability reporting",
        ],
    },
    "Energy": {
        "base_weight": 1.4,
        "keywords": [
            "energy", "oil", "gas", "pipeline", "lng", "nuclear",
            "renewable", "solar", "wind", "electricity", "alberta",
            "upstream", "downstream", "neb", "cema",
        ],
        "phrases": [
            "oil and gas", "energy regulation", "lng project",
            "renewable energy", "energy transition", "pipeline",
            "electricity market", "clean energy",
        ],
    },
    "Financial Services": {
        "base_weight": 1.4,
        "keywords": [
            "banking", "fintech", "insurance", "lending", "osfi",
            "financial institution", "payment", "cryptocurrency",
            "digital asset", "aml", "kyc", "regulatory", "boc",
        ],
        "phrases": [
            "financial services", "banking regulation", "fintech",
            "anti-money laundering", "know your client",
            "payment systems", "digital assets", "open banking",
        ],
    },
    "Competition": {
        "base_weight": 1.4,
        "keywords": [
            "competition", "antitrust", "merger review", "cartel",
            "competition bureau", "abuse of dominance", "price fixing",
            "regulated conduct", "market power",
        ],
        "phrases": [
            "competition law", "competition bureau", "merger review",
            "abuse of dominance", "price fixing", "antitrust",
            "deceptive marketing", "competition act",
        ],
    },
    "Healthcare": {
        "base_weight": 1.3,
        "keywords": [
            "healthcare", "health", "pharmaceutical", "life sciences",
            "medical device", "fda", "health canada", "hospital",
            "biotech", "clinical trial", "drug",
        ],
        "phrases": [
            "healthcare law", "pharmaceutical regulation",
            "life sciences", "health canada", "clinical trial",
            "medical device", "drug approval",
        ],
    },
    "Immigration": {
        "base_weight": 1.4,
        "keywords": [
            "immigration", "work permit", "ircc", "visa", "refugee",
            "permanent residence", "citizenship", "lmia", "intracompany",
            "global talent", "immigration compliance",
        ],
        "phrases": [
            "immigration law", "work permit", "permanent residence",
            "global talent stream", "intracompany transfer",
            "immigration compliance", "refugee law",
        ],
    },
    "Infrastructure": {
        "base_weight": 1.4,
        "keywords": [
            "infrastructure", "p3", "ppp", "construction", "project finance",
            "procurement", "engineering", "transport", "transit",
            "public private", "concession", "municipal",
        ],
        "phrases": [
            "infrastructure projects", "public private partnership",
            "project finance", "p3 procurement", "construction law",
            "infrastructure development", "transit project",
        ],
    },
}

# Flat list of department names (used in various places)
DEPARTMENT_NAMES = list(DEPARTMENTS.keys())
