"""
Taxonomy — keywords and phrases for 17 practice departments.
Phrases (multi-word) get a 2.5× boost over single keywords.
v2: expanded with Canadian-specific terms, new ESG/AI/crypto vocabulary.
"""

DEPARTMENTS = {
    "Corporate/M&A": {
        "keywords": [
            "merger", "acquisition", "m&a", "corporate", "transaction",
            "deal", "takeover", "divestiture", "spinoff", "amalgamation",
            "securities", "shareholder", "governance", "proxy", "going-private",
            "carve-out", "wind-down", "recapitalization", "joint venture",
            "strategic review", "bid", "tender offer", "arrangement", "squeeze-out",
            "rollover", "earnout", "representations", "warranties", "reps",
            "closing", "signing", "definitive agreement", "term sheet",
        ],
        "phrases": [
            "mergers and acquisitions", "corporate governance", "hostile takeover",
            "going private transaction", "purchase agreement", "share purchase",
            "asset purchase", "management buyout", "strategic acquisition",
            "plan of arrangement", "arrangement agreement", "definitive agreement",
            "business combination", "strategic combination", "corporate transaction",
            "change of control", "take-over bid", "fairness opinion",
        ],
    },
    "Private Equity": {
        "keywords": [
            "private equity", "pe fund", "buyout", "portfolio company",
            "venture capital", "growth equity", "fund formation", "lbo",
            "carried interest", "limited partnership", "gp", "lp",
            "secondaries", "co-invest", "continuation fund", "spac",
            "growth capital", "seed round", "series a", "series b",
        ],
        "phrases": [
            "private equity fund", "leveraged buyout", "portfolio company",
            "fund formation", "management fee", "co-investment",
            "general partner", "limited partner", "fund of funds",
            "venture capital fund", "growth equity fund", "buyout fund",
        ],
    },
    "Capital Markets": {
        "keywords": [
            "ipo", "prospectus", "underwriting", "equity offering",
            "debt financing", "bond", "debenture", "tsx", "investment grade",
            "sedar", "continuous disclosure", "exempt distribution",
            "bought deal", "marketed offering", "overnight deal", "atm",
            "convertible", "high yield", "subordinated debt", "note",
            "cdnx", "tsxv", "aif", "mic", "mi 61-101",
        ],
        "phrases": [
            "initial public offering", "capital markets", "bought deal",
            "short form prospectus", "national instrument", "continuous disclosure",
            "equity financing", "debt capital", "base shelf prospectus",
            "at-the-market", "private placement", "reg s", "rule 144a",
            "cross-border offering", "dual-listed", "going public",
        ],
    },
    "Litigation": {
        "keywords": [
            "litigation", "lawsuit", "plaintiff", "defendant", "court",
            "trial", "appeal", "arbitration", "dispute", "injunction",
            "damages", "class action", "settlement", "judgment",
            "mediation", "motion", "leave", "certify", "certiorari",
            "certifying", "stay", "contempt", "discovery", "examination",
            "cross-examination", "evidence", "interlocutory", "costs",
        ],
        "phrases": [
            "class action", "commercial litigation", "appellate court",
            "statement of claim", "summary judgment", "contempt of court",
            "ontario superior court", "federal court", "supreme court canada",
            "court of appeal", "notice of civil claim", "writ of summons",
            "international arbitration", "investor-state arbitration",
        ],
    },
    "Restructuring": {
        "keywords": [
            "restructuring", "insolvency", "bankruptcy", "creditor",
            "ccaa", "bia", "receivership", "monitor", "proposal",
            "debtor", "distressed", "wind-up", "liquidation", "winding-up",
            "trustee", "assignments", "consumer proposal", "court-supervised",
            "pre-packaged", "stalking horse", "363 sale", "sale process",
        ],
        "phrases": [
            "companies creditors arrangement", "court protection",
            "creditor protection", "debt restructuring", "plan of arrangement",
            "interim receiver", "notice of intention", "bankruptcy and insolvency",
            "initial order", "stay of proceedings", "claims process",
            "key employee retention", "dip financing", "debtor in possession",
        ],
    },
    "Real Estate": {
        "keywords": [
            "real estate", "property", "land", "zoning", "development",
            "lease", "condo", "reit", "landlord", "tenant", "conveyance",
            "subdivision", "planning", "commercial property", "residential",
            "industrial", "mixed-use", "strata", "condominium", "easement",
            "servitude", "title", "expropriation", "land transfer",
        ],
        "phrases": [
            "real estate development", "land use planning", "commercial lease",
            "purchase and sale", "real estate investment trust",
            "condominium development", "mixed use development",
            "urban development", "transit-oriented development",
            "purpose-built rental", "land development agreement",
        ],
    },
    "Tax": {
        "keywords": [
            "tax", "taxation", "cra", "income tax", "gst", "hst",
            "transfer pricing", "withholding", "estate planning",
            "treaty", "audit", "reassessment", "objection", "appeal",
            "vat", "customs", "excise", "pst", "qst", "luxury",
            "crypto tax", "digital services tax", "pillar two", "beps",
        ],
        "phrases": [
            "income tax act", "transfer pricing", "tax planning",
            "tax dispute", "canada revenue agency", "general anti-avoidance",
            "tax treaty", "voluntary disclosure", "advance tax ruling",
            "cross-border tax", "international tax", "corporate tax",
            "thin capitalization", "foreign affiliate", "controlled foreign",
        ],
    },
    "Employment": {
        "keywords": [
            "employment", "labour", "human rights", "workplace",
            "wrongful dismissal", "collective agreement", "union",
            "arbitration", "esa", "ohrc", "harassment", "pay equity",
            "termination", "severance", "constructive", "just cause",
            "accommodation", "overtime", "misclassification", "contractor",
            "non-compete", "non-solicitation", "injunction",
        ],
        "phrases": [
            "employment law", "wrongful dismissal", "constructive dismissal",
            "collective bargaining", "human rights tribunal",
            "labour relations board", "ontario labour relations",
            "employment standards", "occupational health", "workplace safety",
            "pay equity plan", "executive compensation", "stock option plan",
        ],
    },
    "IP": {
        "keywords": [
            "intellectual property", "patent", "trademark", "copyright",
            "trade secret", "licensing", "infringement", "brand",
            "cipo", "technology transfer", "industrial design", "trade dress",
            "counterfeiting", "domain", "database rights", "moral rights",
            "prosecution", "patent portfolio", "ip strategy",
        ],
        "phrases": [
            "intellectual property", "trade-mark", "patent application",
            "technology licensing", "ip portfolio", "patent prosecution",
            "trademark registration", "copyright infringement",
            "trade secret misappropriation", "passing off", "domain dispute",
        ],
    },
    "Data Privacy": {
        "keywords": [
            "privacy", "data protection", "cybersecurity", "pipeda",
            "ai regulation", "breach", "personal information", "gdpr",
            "opc", "data governance", "security incident", "law 25",
            "biometrics", "facial recognition", "surveillance", "consent",
            "data localization", "cross-border transfer", "dpa",
            "privacy impact assessment", "pia", "dpia", "cpra",
        ],
        "phrases": [
            "data breach", "privacy commissioner", "personal information",
            "cybersecurity incident", "data protection", "privacy law",
            "artificial intelligence regulation", "privacy impact assessment",
            "privacy by design", "data minimization", "right to erasure",
            "breach notification", "privacy audit", "ai governance",
        ],
    },
    "ESG": {
        "keywords": [
            "esg", "sustainability", "climate", "environment", "emissions",
            "carbon", "indigenous", "reconciliation", "dei", "diversity",
            "social responsibility", "impact", "taxonomy", "tcfd",
            "greenwashing", "net-zero", "carbon credit", "offset",
            "indigenous consultation", "duty to consult", "impact benefit",
        ],
        "phrases": [
            "environmental social governance", "climate disclosure",
            "net zero", "indigenous rights", "diversity equity inclusion",
            "sustainable finance", "climate risk", "transition risk",
            "indigenous consultation", "impact benefit agreement",
            "duty to consult and accommodate", "sustainable development",
            "environmental assessment", "climate change disclosure",
        ],
    },
    "Energy": {
        "keywords": [
            "energy", "oil", "gas", "pipeline", "electricity", "renewable",
            "solar", "wind", "nuclear", "neb", "aeso", "lng",
            "upstream", "downstream", "midstream", "petrochemical",
            "refinery", "transmission", "distribution", "rate",
            "hydrogen", "geothermal", "tidal", "ceb", "oeb",
        ],
        "phrases": [
            "oil and gas", "energy transition", "renewable energy",
            "national energy regulator", "pipeline project",
            "power purchase agreement", "electricity market",
            "clean energy", "hydrogen economy", "carbon capture",
        ],
    },
    "Financial Services": {
        "keywords": [
            "banking", "finance", "fintech", "osfi", "credit", "insurance",
            "derivatives", "hedge fund", "aml", "compliance", "lending",
            "payment", "crypto", "digital asset", "stablecoin",
            "blockchain", "defi", "nft", "tokenization", "cbdc",
            "kyc", "fatf", "fintrac", "mfda", "iiroc", "ciro",
        ],
        "phrases": [
            "financial services", "bank act", "financial institution",
            "anti-money laundering", "digital currency", "crypto currency",
            "payment services", "digital payments", "open banking",
            "buy now pay later", "embedded finance", "regtech",
            "know your client", "proceeds of crime", "terrorist financing",
        ],
    },
    "Competition": {
        "keywords": [
            "competition", "antitrust", "cartel", "merger review",
            "abuse of dominance", "competition bureau", "consent agreement",
            "deceptive marketing", "price fixing", "market allocation",
            "bid rigging", "refusal to deal", "price maintenance",
            "reviewable", "notifiable transaction", "advance ruling",
        ],
        "phrases": [
            "competition act", "merger review", "abuse of dominance",
            "competition bureau", "anti-competitive", "consent order",
            "negotiated resolution", "merger notification",
            "competitive effects", "market definition",
        ],
    },
    "Healthcare": {
        "keywords": [
            "healthcare", "pharmaceutical", "medical", "health law",
            "biotech", "clinical trial", "drug", "patented medicine",
            "health canada", "diagnostic", "device", "mdr",
            "life sciences", "hospital", "physician", "genetic",
            "oncology", "regulatory approval", "din",
        ],
        "phrases": [
            "health law", "pharmaceutical regulatory", "clinical trial",
            "food and drug", "patented medicine", "life sciences",
            "health care system", "medical device", "health canada approval",
        ],
    },
    "Immigration": {
        "keywords": [
            "immigration", "visa", "work permit", "ircc", "refugee",
            "citizenship", "permanent resident", "lmia", "express entry",
            "intracompany", "global talent", "startup visa",
            "provincial nominee", "atlantic immigration", "caregiver",
        ],
        "phrases": [
            "immigration law", "work permit", "permanent residence",
            "labour market impact assessment", "global talent stream",
            "intracompany transfer", "immigration compliance",
        ],
    },
    "Infrastructure": {
        "keywords": [
            "infrastructure", "p3", "ppp", "public private", "construction",
            "transit", "toll road", "airport", "port", "procurement",
            "concession", "afp", "alternative finance", "dbfom",
            "green infrastructure", "social infrastructure",
        ],
        "phrases": [
            "public private partnership", "infrastructure project",
            "design build finance", "project finance", "alternative financing",
            "infrastructure fund", "public procurement", "rfp process",
            "design build", "db finance operate maintain",
        ],
    },
}
