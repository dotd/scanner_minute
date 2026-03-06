# fmt: off
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK.B", "JPM", "V",
    "UNH", "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX", "MRK", "ABBV",
    "LLY", "PEP", "KO", "COST", "AVGO", "MCD", "TMO", "CSCO", "ACN", "ABT",
    "DHR", "CRM", "NEE", "LIN", "TXN", "AMD", "PM", "CMCSA", "NKE", "UPS",
    "INTC", "HON", "ORCL", "AMGN", "RTX", "LOW", "QCOM", "BA", "CAT", "GS",
]

# S&P 500 constituents (approximate as of early 2025 — verify against a live source)
SP500_TICKERS = [
    "A", "AAL", "AAPL", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI",
    "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIZ", "AJG",
    "AKAM", "ALB", "ALGN", "ALL", "ALLE", "AMAT", "AMCR", "AMD", "AME", "AMGN",
    "AMP", "AMT", "AMZN", "ANET", "ANSS", "AON", "AOS", "APA", "APD", "APH",
    "APTV", "ARE", "ATO", "AVB", "AVGO", "AVY", "AWK", "AXP", "AZO",
    "BA", "BAC", "BAX", "BBWI", "BBY", "BDX", "BEN", "BF.B", "BG", "BIIB",
    "BIO", "BK", "BKNG", "BKR", "BLDR", "BLK", "BMY", "BR", "BRK.B", "BRO",
    "BSX", "BWA", "BX", "BXP",
    "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE", "CBRE", "CCI", "CCL",
    "CDNS", "CDW", "CE", "CEG", "CF", "CFG", "CHD", "CHRW", "CHTR", "CI",
    "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC",
    "CNP", "COF", "COO", "COP", "COR", "COST", "CPAY", "CPB", "CPRT", "CPT",
    "CRL", "CRM", "CRWD", "CSCO", "CSGP", "CSX", "CTAS", "CTRA", "CTSH", "CTVA",
    "CVS", "CVX", "CZR",
    "D", "DAL", "DAY", "DD", "DE", "DECK", "DFS", "DG", "DGX", "DHI",
    "DHR", "DIS", "DLR", "DLTR", "DOV", "DOW", "DPZ", "DRI", "DTE", "DUK",
    "DVA", "DVN",
    "DXCM", "EA", "EBAY", "ECL", "ED", "EFX", "EIX", "EL", "EMN", "EMR",
    "ENPH", "EOG", "EPAM", "EQIX", "EQR", "EQT", "ES", "ESS", "ETN", "ETR",
    "EVRG", "EW", "EXC", "EXPD", "EXPE", "EXR",
    "F", "FANG", "FAST", "FBHS", "FCX", "FDS", "FDX", "FE", "FFIV", "FI",
    "FICO", "FIS", "FISV", "FITB", "FLT", "FMC", "FOX", "FOXA", "FRT", "FSLR",
    "FTNT", "FTV",
    "GD", "GDDY", "GE", "GEHC", "GEN", "GEV", "GILD", "GIS", "GL", "GLW",
    "GM", "GNRC", "GOOG", "GOOGL", "GPC", "GPN", "GRMN", "GS", "GWW",
    "HAL", "HAS", "HBAN", "HCA", "HD", "HOLX", "HON", "HPE", "HPQ", "HRL",
    "HSIC", "HST", "HSY", "HUBB", "HUM", "HWM", "HII",
    "IBM", "ICE", "IDXX", "IEX", "IFF", "ILMN", "INCY", "INTC", "INTU", "INVH",
    "IP", "IPG", "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ",
    "J", "JBHT", "JBL", "JCI", "JKHY", "JNJ", "JNPR", "JPM",
    "K", "KDP", "KEY", "KEYS", "KHC", "KIM", "KLAC", "KMB", "KMI", "KMX",
    "KO", "KR",
    "L", "LDOS", "LEN", "LH", "LHX", "LIN", "LKQ", "LLY", "LMT", "LNT",
    "LOW", "LRCX", "LULU", "LUV", "LVS", "LW", "LYB", "LYV",
    "MA", "MAA", "MAR", "MAS", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT",
    "MET", "META", "MGM", "MHK", "MKC", "MKTX", "MLM", "MMC", "MMM", "MNST",
    "MO", "MOH", "MOS", "MPC", "MPWR", "MRK", "MRNA", "MRO", "MS", "MSCI",
    "MSFT", "MSI", "MTB", "MTCH", "MTD", "MU",
    "NCLH", "NDAQ", "NDSN", "NEE", "NEM", "NFLX", "NI", "NKE", "NOC", "NOW",
    "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVDA", "NVR", "NWS", "NWSA",
    "O", "ODFL", "OKE", "OMC", "ON", "ORCL", "ORLY", "OTIS", "OXY",
    "PANW", "PARA", "PAYC", "PAYX", "PCAR", "PCG", "PDD", "PEAK", "PEG", "PEP",
    "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PLD", "PM", "PNC",
    "PNR", "PNW", "POOL", "PPG", "PPL", "PRU", "PSA", "PSX", "PTC", "PVH",
    "PWR", "PXD",
    "QCOM", "QRVO",
    "RCL", "RE", "REG", "REGN", "RF", "RHI", "RJF", "RL", "RMD", "ROK",
    "ROL", "ROP", "ROST", "RSG", "RTX",
    "SBAC", "SBUX", "SCHW", "SEE", "SHW", "SIVB", "SJM", "SLB", "SMCI", "SNA",
    "SNPS", "SO", "SOLV", "SPG", "SPGI", "SRE", "STE", "STLD", "STT", "STX",
    "STZ", "SWK", "SWKS", "SYF", "SYK", "SYY",
    "T", "TAP", "TDG", "TDY", "TECH", "TEL", "TER", "TFC", "TFX", "TGT",
    "TJX", "TMO", "TMUS", "TPR", "TRGP", "TRMB", "TROW", "TRV", "TSCO", "TSLA",
    "TSN", "TT", "TTWO", "TXN", "TXT", "TYL",
    "UAL", "UBER", "UDR", "UHS", "ULTA", "UNH", "UNP", "UPS", "URI", "USB",
    "V", "VICI", "VLO", "VLTO", "VMC", "VRSK", "VRSN", "VRTX", "VST", "VTR",
    "VTRS", "VZ",
    "WAB", "WAT", "WBA", "WBD", "WDC", "WEC", "WELL", "WFC", "WHR", "WM",
    "WMB", "WMT", "WRB", "WST", "WTW", "WY", "WYNN",
    "XEL", "XOM", "XRAY", "XYL",
    "YUM",
    "ZBH", "ZBRA", "ZION", "ZTS",
]

# NASDAQ-100 constituents (approximate as of early 2025 — verify against a live source)
NASDAQ100_TICKERS = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD", "AMGN",
    "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AZN",
    "BIIB", "BKNG", "BKR",
    "CCEP", "CDNS", "CDW", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO",
    "CSGP", "CSX", "CTAS", "CTSH",
    "DASH", "DDOG", "DLTR", "DXCM",
    "EA", "EXC",
    "FANG", "FAST", "FTNT",
    "GEHC", "GFS", "GILD", "GOOG", "GOOGL",
    "HON",
    "IDXX", "ILMN", "INTC", "INTU", "ISRG",
    "KDP", "KHC", "KLAC",
    "LIN", "LRCX", "LULU",
    "MAR", "MCHP", "MDB", "MDLZ", "MELI", "META", "MNST", "MRNA", "MRVL", "MSFT", "MU",
    "NFLX", "NVDA", "NXPI",
    "ODFL", "ON", "ORLY",
    "PANW", "PAYX", "PCAR", "PDD", "PEP", "PYPL",
    "QCOM",
    "REGN", "ROST",
    "SBUX", "SMCI", "SNPS", "SPLK",
    "TEAM", "TMUS", "TSLA", "TTD", "TTWO", "TXN",
    "VRSK", "VRTX",
    "WBA", "WBD", "WDAY",
    "XEL",
    "ZS",
]

# Combined S&P 500 + NASDAQ-100, deduplicated and sorted
SP500_AND_NASDAQ100_TICKERS = sorted(set(SP500_TICKERS + NASDAQ100_TICKERS))

ETF_TICKERS = [
    # --- Broad Market ---
    "SPY",   # SPDR S&P 500 ETF Trust
    "IVV",   # iShares Core S&P 500 ETF
    "VOO",   # Vanguard S&P 500 ETF
    "VTI",   # Vanguard Total Stock Market ETF
    "QQQ",   # Invesco QQQ Trust (NASDAQ-100)
    "QQQM",  # Invesco NASDAQ 100 ETF (lower expense ratio)
    "DIA",   # SPDR Dow Jones Industrial Average ETF
    "IWM",   # iShares Russell 2000 ETF (small cap)
    "IWF",   # iShares Russell 1000 Growth ETF
    "IWD",   # iShares Russell 1000 Value ETF
    "MDY",   # SPDR S&P MidCap 400 ETF
    "IJR",   # iShares Core S&P Small-Cap ETF
    "IJH",   # iShares Core S&P Mid-Cap ETF
    "RSP",   # Invesco S&P 500 Equal Weight ETF
    "VTV",   # Vanguard Value ETF
    "VUG",   # Vanguard Growth ETF
    "SCHD",  # Schwab U.S. Dividend Equity ETF
    "VIG",   # Vanguard Dividend Appreciation ETF
    "DVY",   # iShares Select Dividend ETF
    "DGRO",  # iShares Core Dividend Growth ETF
    # --- Sector ETFs ---
    "XLK",   # Technology Select Sector SPDR
    "XLF",   # Financial Select Sector SPDR
    "XLV",   # Health Care Select Sector SPDR
    "XLE",   # Energy Select Sector SPDR
    "XLI",   # Industrial Select Sector SPDR
    "XLY",   # Consumer Discretionary Select Sector SPDR
    "XLP",   # Consumer Staples Select Sector SPDR
    "XLU",   # Utilities Select Sector SPDR
    "XLB",   # Materials Select Sector SPDR
    "XLRE",  # Real Estate Select Sector SPDR
    "XLC",   # Communication Services Select Sector SPDR
    "VGT",   # Vanguard Information Technology ETF
    "VHT",   # Vanguard Health Care ETF
    "VFH",   # Vanguard Financials ETF
    "VDE",   # Vanguard Energy ETF
    "VIS",   # Vanguard Industrials ETF
    "VCR",   # Vanguard Consumer Discretionary ETF
    "VDC",   # Vanguard Consumer Staples ETF
    "VNQ",   # Vanguard Real Estate ETF
    # --- Thematic / Industry ---
    "ARKK",  # ARK Innovation ETF
    "ARKW",  # ARK Next Generation Internet ETF
    "ARKG",  # ARK Genomic Revolution ETF
    "ARKF",  # ARK Fintech Innovation ETF
    "SMH",   # VanEck Semiconductor ETF
    "SOXX",  # iShares Semiconductor ETF
    "XBI",   # SPDR S&P Biotech ETF
    "IBB",   # iShares Biotechnology ETF
    "IYR",   # iShares U.S. Real Estate ETF
    "ITB",   # iShares U.S. Home Construction ETF
    "XHB",   # SPDR S&P Homebuilders ETF
    "KRE",   # SPDR S&P Regional Banking ETF
    "KBE",   # SPDR S&P Bank ETF
    "XOP",   # SPDR S&P Oil & Gas Exploration & Production ETF
    "OIH",   # VanEck Oil Services ETF
    "GDX",   # VanEck Gold Miners ETF
    "GDXJ",  # VanEck Junior Gold Miners ETF
    "SLV",   # iShares Silver Trust
    "GLD",   # SPDR Gold Shares
    "IAU",   # iShares Gold Trust
    "HACK",  # ETFMG Prime Cyber Security ETF
    "CIBR",  # First Trust NASDAQ Cybersecurity ETF
    "BOTZ",  # Global X Robotics & AI ETF
    "ROBO",  # Robo Global Robotics & Automation ETF
    "LIT",   # Global X Lithium & Battery Tech ETF
    "TAN",   # Invesco Solar ETF
    "ICLN",  # iShares Global Clean Energy ETF
    "QCLN",  # First Trust NASDAQ Clean Edge Green Energy ETF
    "JETS",  # U.S. Global Jets ETF
    "KWEB",  # KraneShares CSI China Internet ETF
    "MCHI",  # iShares MSCI China ETF
    # --- International ---
    "VEA",   # Vanguard FTSE Developed Markets ETF
    "VWO",   # Vanguard FTSE Emerging Markets ETF
    "EFA",   # iShares MSCI EAFE ETF (developed ex-US)
    "EEM",   # iShares MSCI Emerging Markets ETF
    "IEMG",  # iShares Core MSCI Emerging Markets ETF
    "VXUS",  # Vanguard Total International Stock ETF
    "EWJ",   # iShares MSCI Japan ETF
    "EWG",   # iShares MSCI Germany ETF
    "EWU",   # iShares MSCI United Kingdom ETF
    "EWZ",   # iShares MSCI Brazil ETF
    "FXI",   # iShares China Large-Cap ETF
    "INDA",  # iShares MSCI India ETF
    "EWT",   # iShares MSCI Taiwan ETF
    "EWY",   # iShares MSCI South Korea ETF
    # --- Fixed Income / Bonds ---
    "AGG",   # iShares Core U.S. Aggregate Bond ETF
    "BND",   # Vanguard Total Bond Market ETF
    "TLT",   # iShares 20+ Year Treasury Bond ETF
    "IEF",   # iShares 7-10 Year Treasury Bond ETF
    "SHY",   # iShares 1-3 Year Treasury Bond ETF
    "TIP",   # iShares TIPS Bond ETF
    "LQD",   # iShares iBoxx $ Investment Grade Corporate Bond ETF
    "HYG",   # iShares iBoxx $ High Yield Corporate Bond ETF
    "JNK",   # SPDR Bloomberg High Yield Bond ETF
    "MUB",   # iShares National Muni Bond ETF
    "BNDX",  # Vanguard Total International Bond ETF
    "EMB",   # iShares J.P. Morgan USD Emerging Markets Bond ETF
    "VCSH",  # Vanguard Short-Term Corporate Bond ETF
    "VCIT",  # Vanguard Intermediate-Term Corporate Bond ETF
    "VGSH",  # Vanguard Short-Term Treasury ETF
    "VGIT",  # Vanguard Intermediate-Term Treasury ETF
    "VGLT",  # Vanguard Long-Term Treasury ETF
    "BIL",   # SPDR Bloomberg 1-3 Month T-Bill ETF
    "SHV",   # iShares Short Treasury Bond ETF
    # --- Leveraged & Inverse ---
    "TQQQ",  # ProShares UltraPro QQQ (3x NASDAQ-100)
    "SQQQ",  # ProShares UltraPro Short QQQ (-3x NASDAQ-100)
    "SPXL",  # Direxion Daily S&P 500 Bull 3X
    "SPXS",  # Direxion Daily S&P 500 Bear 3X
    "UPRO",  # ProShares UltraPro S&P 500 (3x)
    "SSO",   # ProShares Ultra S&P 500 (2x)
    "SDS",   # ProShares UltraShort S&P 500 (-2x)
    "QLD",   # ProShares Ultra QQQ (2x NASDAQ-100)
    "SOXL",  # Direxion Daily Semiconductor Bull 3X
    "SOXS",  # Direxion Daily Semiconductor Bear 3X
    "LABU",  # Direxion Daily S&P Biotech Bull 3X
    "LABD",  # Direxion Daily S&P Biotech Bear 3X
    "TNA",   # Direxion Daily Small Cap Bull 3X
    "TZA",   # Direxion Daily Small Cap Bear 3X
    "UVXY",  # ProShares Ultra VIX Short-Term Futures (1.5x)
    "SVXY",  # ProShares Short VIX Short-Term Futures (-0.5x)
    # --- Volatility ---
    "VXX",   # iPath Series B S&P 500 VIX Short-Term Futures ETN
    "VIXY",  # ProShares VIX Short-Term Futures ETF
    # --- Commodities ---
    "USO",   # United States Oil Fund
    "UNG",   # United States Natural Gas Fund
    "DBA",   # Invesco DB Agriculture Fund
    "DBC",   # Invesco DB Commodity Index Tracking Fund
    "PDBC",  # Invesco Optimum Yield Diversified Commodity Strategy
    "CORN",  # Teucrium Corn Fund
    "WEAT",  # Teucrium Wheat Fund
    "SOYB",  # Teucrium Soybean Fund
    # --- Currency ---
    "UUP",   # Invesco DB US Dollar Index Bullish Fund
    "FXE",   # Invesco CurrencyShares Euro Trust
    "FXY",   # Invesco CurrencyShares Japanese Yen Trust
    "FXB",   # Invesco CurrencyShares British Pound Trust
]
# fmt: on
