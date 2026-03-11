"""
Practice department taxonomy.
Each department has:
  - keywords: single-word signals (weight 1.0)
  - phrases:  multi-word signals (weight 2.5x boost)
  - courts:   CanLII court codes that map directly here
"""

DEPARTMENTS = [
    {
        "name": "Corporate / M&A",
        "keywords": [
            "merger", "acquisition", "takeover", "amalgamation", "consolidation",
            "corporate", "governance", "shareholder", "buyout", "divestiture",
            "m&a", "transaction", "deal", "target", "acquiror", "bidder",
            "arrangement", "plan", "going-private", "proxy", "circular",
        ],
        "phrases": [
            "mergers and acquisitions", "business combination", "share purchase",
            "asset purchase", "plan of arrangement", "going private transaction",
            "corporate governance", "shareholder rights", "takeover bid",
            "insider trading", "poison pill", "rights plan", "special committee",
            "fairness opinion", "strategic review", "board of directors",
            "cross-border m&a", "public m&a", "private m&a",
        ],
    },
    {
        "name": "Private Equity",
        "keywords": [
            "private equity", "buyout", "lbo", "portfolio", "fund",
            "gp", "lp", "carried", "vintage", "co-invest",
            "management buyout", "sponsor", "recapitalization",
        ],
        "phrases": [
            "private equity fund", "leveraged buyout", "management buyout",
            "growth equity", "venture capital", "fund formation",
            "limited partnership", "general partner", "carried interest",
            "co-investment", "secondary transaction", "portfolio company",
            "sponsor-backed", "private capital",
        ],
    },
    {
        "name": "Capital Markets",
        "keywords": [
            "securities", "ipo", "prospectus", "underwriting", "offering",
            "equity", "debt", "bond", "debenture", "convertible",
            "osc", "securities commission", "disclosure", "continuous",
            "listing", "tsx", "tsx-v", "aif", "mda", "sedar",
        ],
        "phrases": [
            "initial public offering", "capital markets", "public offering",
            "private placement", "bought deal", "short form prospectus",
            "shelf prospectus", "secondary offering", "flow-through shares",
            "securities regulation", "take-over bid circular", "material change",
            "continuous disclosure", "national instrument", "exempt market",
            "investment fund", "structured finance", "securitization",
        ],
    },
    {
        "name": "Litigation & Disputes",
        "keywords": [
            "litigation", "dispute", "trial", "appeal", "plaintiff",
            "defendant", "injunction", "judgment", "damages", "tort",
            "class action", "arbitration", "mediation", "evidence",
            "cross-examination", "discovery", "examination",
        ],
        "phrases": [
            "commercial litigation", "class action", "civil litigation",
            "appellate advocacy", "dispute resolution", "court of appeal",
            "superior court", "federal court", "supreme court",
            "injunctive relief", "summary judgment", "contempt of court",
            "breach of contract", "breach of fiduciary duty", "fraud litigation",
            "securities litigation", "product liability", "defamation",
        ],
    },
    {
        "name": "Restructuring & Insolvency",
        "keywords": [
            "insolvency", "restructuring", "ccaa", "bia", "bankruptcy",
            "monitor", "receiver", "creditor", "debtor", "claim",
            "liquidation", "winding", "proposal", "arrangement",
            "distressed", "turnaround", "workout",
        ],
        "phrases": [
            "companies creditors arrangement act", "bankruptcy and insolvency act",
            "court-appointed monitor", "receivership proceeding",
            "debt restructuring", "financial restructuring", "creditor protection",
            "claims process", "plan of compromise", "stalking horse",
            "363 sale", "cross-border insolvency", "chapter 15",
            "distressed asset", "out-of-court restructuring",
        ],
    },
    {
        "name": "Real Estate",
        "keywords": [
            "real estate", "property", "land", "lease", "landlord",
            "tenant", "zoning", "development", "condo", "reit",
            "mortgage", "title", "conveyancing", "easement",
        ],
        "phrases": [
            "commercial real estate", "real property", "real estate investment",
            "real estate investment trust", "land transfer", "title insurance",
            "development agreement", "subdivision", "rezoning application",
            "official plan amendment", "site plan approval", "construction lien",
            "commercial lease", "ground lease", "sale-leaseback",
            "joint venture real estate", "proptech",
        ],
    },
    {
        "name": "Tax",
        "keywords": [
            "tax", "taxation", "income tax", "gst", "hst", "customs",
            "transfer pricing", "treaty", "withholding", "cra",
            "avoidance", "evasion", "reassessment", "objection",
        ],
        "phrases": [
            "income tax act", "transfer pricing", "international tax",
            "corporate tax", "indirect tax", "tax planning",
            "tax litigation", "tax controversy", "cra audit",
            "advance tax ruling", "general anti-avoidance rule", "gaar",
            "voluntary disclosure", "tax treaty", "permanent establishment",
            "foreign affiliate", "controlled foreign corporation",
            "film tax credit", "scientific research",
        ],
    },
    {
        "name": "Employment & Labour",
        "keywords": [
            "employment", "labour", "labor", "employee", "employer",
            "union", "collective", "bargaining", "wrongful dismissal",
            "termination", "human rights", "discrimination", "harassment",
            "occupational health", "safety", "workers compensation",
        ],
        "phrases": [
            "employment law", "labour relations", "wrongful dismissal",
            "constructive dismissal", "human rights", "employment standards",
            "collective agreement", "labour arbitration", "union organizing",
            "workplace investigation", "occupational health and safety",
            "pay equity", "accommodation duty", "severance package",
            "non-compete agreement", "executive compensation",
        ],
    },
    {
        "name": "Intellectual Property",
        "keywords": [
            "intellectual property", "ip", "patent", "trademark", "copyright",
            "trade secret", "licensing", "royalty", "infringement",
            "passing off", "industrial design", "plant breeders",
        ],
        "phrases": [
            "intellectual property", "patent litigation", "trademark registration",
            "copyright infringement", "trade secret", "ip licensing",
            "technology transfer", "patent prosecution", "trademark opposition",
            "domain name dispute", "brand protection", "counterfeit",
            "pharmaceutical patent", "patent linkage",
        ],
    },
    {
        "name": "Data Privacy & Cybersecurity",
        "keywords": [
            "privacy", "data", "cybersecurity", "breach", "pipeda", "casl",
            "gdpr", "personal information", "consent", "commissioner",
            "cyber incident", "ransomware", "phishing", "ai governance",
        ],
        "phrases": [
            "data privacy", "privacy law", "cybersecurity", "data breach",
            "privacy commissioner", "personal information protection",
            "privacy impact assessment", "data governance", "ai regulation",
            "privacy breach", "cyber incident response", "threat intelligence",
            "privacy by design", "cross-border data transfer",
            "biometric data", "law 25", "bill c-27", "consumer privacy",
        ],
    },
    {
        "name": "ESG & Regulatory",
        "keywords": [
            "esg", "environment", "climate", "sustainability", "carbon",
            "emissions", "regulatory", "compliance", "administrative",
            "government", "public law", "constitutional", "judicial review",
        ],
        "phrases": [
            "environmental law", "esg", "climate change", "carbon pricing",
            "sustainability reporting", "environmental assessment",
            "impact assessment", "net zero", "green finance",
            "emissions trading", "carbon offset", "environmental compliance",
            "regulatory approval", "judicial review", "administrative tribunal",
            "regulatory affairs", "government relations",
        ],
    },
    {
        "name": "Energy & Natural Resources",
        "keywords": [
            "energy", "oil", "gas", "mining", "petroleum", "lng",
            "pipeline", "renewable", "wind", "solar", "nuclear",
            "hydroelectric", "electricity", "natural resources",
            "crown", "indigenous", "resource",
        ],
        "phrases": [
            "oil and gas", "natural gas", "lng", "energy law",
            "mining law", "natural resources", "pipeline project",
            "renewable energy", "energy transition", "power purchase agreement",
            "electricity regulation", "nuclear energy", "upstream oil",
            "midstream", "downstream", "royalty regime",
            "indigenous consultation", "free prior informed consent",
            "duty to consult", "resource project", "energy regulatory",
        ],
    },
    {
        "name": "Financial Services & Regulatory",
        "keywords": [
            "banking", "financial", "fintech", "payment", "insurance",
            "osfi", "bank act", "credit union", "deposit", "lending",
            "derivative", "swap", "hedge fund", "aml", "fintrac",
        ],
        "phrases": [
            "financial services", "banking regulation", "fintech",
            "payment systems", "insurance law", "anti-money laundering",
            "know your customer", "kyc", "financial institution",
            "bank act compliance", "open banking", "digital assets",
            "cryptocurrency regulation", "defi", "stablecoin",
            "clearing and settlement", "osfi guideline", "prudential regulation",
        ],
    },
    {
        "name": "Competition & Antitrust",
        "keywords": [
            "competition", "antitrust", "merger review", "bureau",
            "cartel", "price fixing", "dominance", "abuse", "consent",
            "remedies", "divestiture", "market power",
        ],
        "phrases": [
            "competition law", "merger review", "competition bureau",
            "competition act", "abuse of dominance", "price fixing",
            "cartel investigation", "market definition", "efficiencies defence",
            "consent agreement", "deceptive marketing", "misleading advertising",
            "reviewable matters", "strategic alliance", "joint venture review",
            "foreign investment review", "investment canada",
        ],
    },
    {
        "name": "Healthcare & Life Sciences",
        "keywords": [
            "health", "pharma", "pharmaceutical", "biotech", "medical",
            "clinical", "fda", "health canada", "drug", "device",
            "regulatory approval", "hospital", "physician",
        ],
        "phrases": [
            "healthcare law", "life sciences", "pharmaceutical law",
            "health canada regulatory", "clinical trial", "drug approval",
            "medical device", "patent linkage", "data exclusivity",
            "hospital law", "public health", "cannabis regulation",
            "psychedelics", "biotech transaction", "rare disease",
        ],
    },
    {
        "name": "Immigration",
        "keywords": [
            "immigration", "visa", "work permit", "lmia", "ircc",
            "citizenship", "refugee", "asylum", "deportation",
            "permanent residency", "express entry",
        ],
        "phrases": [
            "immigration law", "corporate immigration", "work permit",
            "labour market impact assessment", "express entry",
            "provincial nominee program", "investor immigration",
            "intra-company transfer", "global talent stream",
            "refugee claim", "humanitarian and compassionate",
            "inadmissibility", "immigration compliance",
        ],
    },
    {
        "name": "Infrastructure & Projects",
        "keywords": [
            "infrastructure", "construction", "p3", "ppp", "project finance",
            "procurement", "concession", "tolling", "transit", "highway",
            "hospital", "school", "bond", "lender",
        ],
        "phrases": [
            "infrastructure law", "project finance", "public private partnership",
            "p3 project", "construction law", "construction lien",
            "design-build", "engineering procurement construction",
            "epc contract", "offtake agreement", "concession agreement",
            "availability payment", "social infrastructure", "transportation",
            "alternative finance and procurement", "afp",
        ],
    },
    # ── Modern / Emerging Practice Areas ─────────────────────────────────────
    {
        "name": "Technology & AI Law",
        "keywords": [
            "artificial intelligence", "ai", "machine learning", "algorithm",
            "automation", "software", "saas", "platform", "cloud", "data",
            "technology", "tech", "digital", "cybersecurity", "cyber",
            "privacy", "gdpr", "pipeda", "biometric", "facial recognition",
            "autonomous", "robotics", "internet of things", "iot",
        ],
        "phrases": [
            "artificial intelligence law", "ai regulation", "ai governance",
            "technology law", "data governance", "privacy law",
            "cyber law", "cybersecurity law", "data breach",
            "software licensing", "tech transactions", "platform liability",
            "algorithmic accountability", "responsible ai",
            "ai ethics", "machine learning model", "generative ai",
            "large language model", "llm", "chatgpt", "openai",
            "digital transformation", "technology transaction",
            "data residency", "cloud computing law",
        ],
    },
    {
        "name": "Crypto & Digital Assets",
        "keywords": [
            "crypto", "cryptocurrency", "bitcoin", "ethereum", "blockchain",
            "token", "nft", "defi", "dao", "stablecoin", "cbdc",
            "digital asset", "exchange", "wallet", "mining",
            "distributed ledger", "web3", "metaverse",
        ],
        "phrases": [
            "cryptocurrency law", "digital assets", "crypto regulation",
            "blockchain technology", "token offering", "security token",
            "utility token", "nft law", "defi protocol", "dao governance",
            "crypto exchange", "virtual currency", "digital currency",
            "central bank digital currency", "crypto compliance",
            "securities token", "initial coin offering", "ico",
            "crypto tax", "blockchain smart contract",
        ],
    },
    {
        "name": "International Trade & Investment",
        "keywords": [
            "trade", "investment", "tariff", "import", "export", "customs",
            "sanctions", "wto", "fta", "usmca", "nafta", "ceta",
            "foreign investment", "ipa", "ics", "dumping", "countervail",
        ],
        "phrases": [
            "international trade", "trade law", "trade remedy",
            "anti-dumping", "countervailing duty", "safeguard measure",
            "investment arbitration", "investor-state", "investment treaty",
            "foreign investment review", "investment canada act",
            "export control", "trade sanctions", "economic sanctions",
            "usmca", "ceta", "cptpp", "customs law", "rules of origin",
            "free trade agreement", "trade dispute",
        ],
    },
    {
        "name": "General",
        "keywords": [
            "law firm", "lawyer", "legal", "counsel", "attorney",
            "practice", "firm", "partner", "associate",
        ],
        "phrases": [
            "law firm", "legal services", "legal team", "legal practice",
            "legal counsel", "general counsel", "in-house counsel",
        ],
    },
]

# Build fast lookup maps
DEPT_NAMES = [d["name"] for d in DEPARTMENTS]
DEPT_BY_NAME = {d["name"]: d for d in DEPARTMENTS}
