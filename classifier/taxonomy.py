"""
Practice area taxonomy — 17 departments with keyword signals.
Edit keywords here to tune classification accuracy.

Each department has:
  keywords   — single-word signals (weight 1.0 each)
  phrases    — multi-word signals (weight 2.5x each)
"""

TAXONOMY = {
    "Corporate / M&A": {
        "keywords": [
            "acquisition", "merger", "takeover", "buyout", "divestiture",
            "transaction", "corporate", "shareholder", "governance", "proxy",
            "due diligence", "closing", "target", "acquiror", "amalgamation",
            "plan of arrangement", "deal", "M&A",
        ],
        "phrases": [
            "mergers and acquisitions", "plan of arrangement", "take-private",
            "going private", "strategic transaction", "purchase agreement",
            "share purchase", "asset purchase", "definitive agreement",
            "board of directors", "special committee",
        ],
    },
    "Private Equity": {
        "keywords": [
            "private equity", "PE", "buyout", "portfolio", "fund",
            "carried interest", "LP", "GP", "management buyout", "MBO",
            "leverage", "LBO", "recapitalization",
        ],
        "phrases": [
            "private equity fund", "leveraged buyout", "management buyout",
            "portfolio company", "fund formation", "GP-LP", "co-investment",
            "private capital", "growth equity",
        ],
    },
    "Capital Markets": {
        "keywords": [
            "IPO", "securities", "prospectus", "equity", "debt", "bond",
            "offering", "underwriting", "issuance", "TSX", "NYSE", "OSC",
            "continuous disclosure", "NI 51-102", "NI 44-101",
        ],
        "phrases": [
            "initial public offering", "public offering", "bought deal",
            "overnight marketed deal", "capital markets", "securities law",
            "continuous disclosure", "exempt market", "accredited investor",
            "offering memorandum", "shelf prospectus",
        ],
    },
    "Litigation & Disputes": {
        "keywords": [
            "litigation", "dispute", "trial", "appeal", "plaintiff", "defendant",
            "injunction", "damages", "judgment", "claim", "lawsuit", "court",
            "arbitration", "mediation", "class action", "hearing",
        ],
        "phrases": [
            "class action", "commercial litigation", "civil litigation",
            "appellate advocacy", "summary judgment", "statement of claim",
            "statement of defence", "jury trial", "cross-examination",
            "court of appeal",
        ],
    },
    "Restructuring & Insolvency": {
        "keywords": [
            "insolvency", "restructuring", "CCAA", "BIA", "bankruptcy",
            "receivership", "monitor", "creditor", "debtor", "workout",
            "distressed", "insolvent",
        ],
        "phrases": [
            "Companies' Creditors Arrangement Act", "Bankruptcy and Insolvency Act",
            "court-appointed monitor", "creditor protection", "restructuring plan",
            "proposal proceedings", "receivership proceedings", "distressed debt",
            "debtor-in-possession",
        ],
    },
    "Real Estate": {
        "keywords": [
            "real estate", "property", "land", "lease", "landlord", "tenant",
            "zoning", "development", "REIT", "condominium", "construction",
            "title", "mortgage", "easement",
        ],
        "phrases": [
            "real estate investment trust", "commercial lease", "commercial real estate",
            "purchase and sale", "title insurance", "land transfer", "site plan",
            "development permit", "mixed-use", "joint venture real estate",
        ],
    },
    "Tax": {
        "keywords": [
            "tax", "taxation", "ITA", "GST", "HST", "transfer pricing",
            "CRA", "audit", "reassessment", "treaty", "withholding",
            "income tax", "estate tax",
        ],
        "phrases": [
            "Income Tax Act", "tax planning", "tax structuring", "transfer pricing",
            "tax controversy", "objection and appeal", "advance tax ruling",
            "voluntary disclosure", "international tax", "indirect tax",
        ],
    },
    "Employment & Labour": {
        "keywords": [
            "employment", "labour", "employee", "employer", "union", "collective",
            "wrongful dismissal", "termination", "human rights", "discrimination",
            "workplace", "arbitration", "OLRB",
        ],
        "phrases": [
            "employment law", "labour relations", "wrongful dismissal",
            "constructive dismissal", "collective agreement", "human rights code",
            "occupational health and safety", "employment standards",
            "labour arbitration", "WSIB", "employment contract",
        ],
    },
    "Intellectual Property": {
        "keywords": [
            "intellectual property", "IP", "patent", "trademark", "copyright",
            "trade secret", "licensing", "infringement", "CIPO", "brand",
            "innovation", "invention",
        ],
        "phrases": [
            "intellectual property", "patent prosecution", "trademark registration",
            "copyright licensing", "trade secret", "IP strategy",
            "technology licensing", "know-how", "patent infringement",
            "trademark opposition",
        ],
    },
    "Data Privacy & Cybersecurity": {
        "keywords": [
            "privacy", "data", "PIPEDA", "CASL", "cybersecurity", "breach",
            "GDPR", "personal information", "surveillance", "encryption",
            "incident response",
        ],
        "phrases": [
            "data privacy", "data protection", "privacy law", "cybersecurity law",
            "data breach", "personal information protection", "privacy commissioner",
            "privacy impact assessment", "PIPEDA compliance", "Bill C-27",
            "incident response", "cyber incident",
        ],
    },
    "ESG & Regulatory": {
        "keywords": [
            "ESG", "environmental", "sustainability", "climate", "regulatory",
            "compliance", "government", "administrative", "Indigenous",
            "reconciliation", "impact assessment",
        ],
        "phrases": [
            "environmental law", "ESG reporting", "sustainability disclosure",
            "climate risk", "impact assessment", "regulatory compliance",
            "administrative law", "Indigenous consultation", "duty to consult",
            "carbon pricing", "net zero",
        ],
    },
    "Energy & Natural Resources": {
        "keywords": [
            "energy", "oil", "gas", "mining", "pipeline", "renewable",
            "electricity", "NEB", "AER", "CER", "upstream", "downstream",
            "extraction", "royalty",
        ],
        "phrases": [
            "oil and gas", "natural resources", "energy law", "energy regulation",
            "pipeline project", "renewable energy", "power purchase agreement",
            "mining law", "resource development", "energy transition",
            "Canada Energy Regulator", "Alberta Energy Regulator",
        ],
    },
    "Financial Services & Regulatory": {
        "keywords": [
            "banking", "financial", "OSFI", "fintech", "payments", "lending",
            "insurance", "AML", "KYC", "regulatory", "FINTRAC",
        ],
        "phrases": [
            "financial services", "banking law", "financial regulation",
            "anti-money laundering", "know your client", "fintech regulation",
            "payment systems", "financial institution", "prudential regulation",
            "Open Banking", "digital assets",
        ],
    },
    "Competition & Antitrust": {
        "keywords": [
            "competition", "antitrust", "merger review", "cartel", "abuse",
            "dominant", "bureau", "consent agreement", "market power",
        ],
        "phrases": [
            "competition law", "Competition Bureau", "merger review",
            "abuse of dominant position", "price-fixing", "cartel investigation",
            "Competition Act", "deceptive marketing", "competitor collaboration",
        ],
    },
    "Healthcare & Life Sciences": {
        "keywords": [
            "healthcare", "health", "pharmaceutical", "drug", "medical device",
            "life sciences", "biotech", "FDA", "Health Canada", "clinical",
            "regulatory approval", "hospital",
        ],
        "phrases": [
            "health law", "life sciences", "pharmaceutical regulation",
            "medical device", "Health Canada approval", "clinical trial",
            "drug approval", "healthcare regulatory", "biopharmaceutical",
        ],
    },
    "Immigration": {
        "keywords": [
            "immigration", "visa", "permit", "citizenship", "refugee",
            "IRCC", "CBSA", "work permit", "PR", "permanent resident",
            "inadmissibility",
        ],
        "phrases": [
            "immigration law", "work permit", "permanent residence",
            "temporary foreign worker", "refugee claim", "inadmissibility",
            "immigration compliance", "LMIA", "Express Entry", "corporate immigration",
        ],
    },
    "Infrastructure & Projects": {
        "keywords": [
            "infrastructure", "project finance", "P3", "PPP", "construction",
            "public-private", "procurement", "concession", "design-build",
        ],
        "phrases": [
            "infrastructure project", "project finance", "public-private partnership",
            "P3 project", "design-build-finance", "construction law",
            "procurement law", "concession agreement", "infrastructure fund",
        ],
    },
}
