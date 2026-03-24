import logging
import time
from datetime import datetime, timezone, timedelta

from ScannerMinute.src.polygon_utils import get_polygon_client


def _get(obj, field, default=None):
    """Get a field from an object that may be a dict or have attributes."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(field, default)
    return getattr(obj, field, default)


def _get_last_close(client, option_ticker, lookback_days=14):
    """Fetch the most recent close price for an option contract via daily aggs."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )
    try:
        bars = list(
            client.get_aggs(option_ticker, 1, "day", from_date, today, limit=10)
        )
        if bars:
            last_bar = bars[-1]
            return {
                "close": last_bar.close,
                "high": last_bar.high,
                "low": last_bar.low,
                "volume": last_bar.volume or 0,
                "vwap": getattr(last_bar, "vwap", None),
            }
    except Exception as e:
        logging.debug(f"No aggs for {option_ticker}: {e}")
    return None


def get_options_chain(
    ticker,
    expiration_date=None,
    contract_type=None,
    strike_price_gte=None,
    strike_price_lte=None,
    client=None,
):
    """
    Fetch the options chain for a ticker using contract listing + daily aggs.

    Returns a list of dicts sorted by (contract_type, strike), each with:
        strike, contract_type, expiration_date, last_close, volume, contract_ticker
    """
    if client is None:
        client = get_polygon_client()

    kwargs = {
        "underlying_ticker": ticker,
        "expired": False,
        "limit": 250,
        "sort": "strike_price",
        "order": "asc",
    }
    if expiration_date:
        kwargs["expiration_date"] = expiration_date
    if contract_type:
        kwargs["contract_type"] = contract_type
    if strike_price_gte is not None:
        kwargs["strike_price_gte"] = strike_price_gte
    if strike_price_lte is not None:
        kwargs["strike_price_lte"] = strike_price_lte

    contracts = list(client.list_options_contracts(**kwargs))
    logging.info(f"Found {len(contracts)} option contracts for {ticker}")

    chain = []
    for i, contract in enumerate(contracts):
        option_ticker = contract.ticker
        if i > 0 and i % 5 == 0:
            time.sleep(1)  # avoid rate limiting on free plan
        agg = _get_last_close(client, option_ticker)

        chain.append(
            {
                "strike": contract.strike_price,
                "contract_type": contract.contract_type,
                "expiration_date": str(contract.expiration_date),
                "last_close": agg["close"] if agg else None,
                "volume": agg["volume"] if agg else 0,
                "contract_ticker": option_ticker,
            }
        )

    chain.sort(key=lambda c: (c["contract_type"] or "", c["strike"] or 0))
    logging.info(f"Fetched prices for {len(chain)} contracts for {ticker}")
    return chain


def get_nearest_expiration(ticker, client=None):
    """
    Find the nearest available expiration date for a ticker's options.
    Returns a date string 'YYYY-MM-DD' or None.
    """
    if client is None:
        client = get_polygon_client()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    contracts = client.list_options_contracts(
        underlying_ticker=ticker,
        expiration_date_gte=today,
        expired=False,
        limit=1,
        sort="expiration_date",
        order="asc",
    )

    for contract in contracts:
        exp = getattr(contract, "expiration_date", None)
        if exp:
            return str(exp)
    return None


def _get_stock_price(client, ticker):
    """Get the latest stock close price via daily aggs."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
    bars = list(client.get_aggs(ticker, 1, "day", from_date, today, limit=10))
    if bars:
        return bars[-1].close
    return None


def analyze_protective_puts(
    ticker,
    num_shares,
    num_contracts,
    stock_price=None,
    expiration_date=None,
    strike_range_pct=20,
    client=None,
):
    """
    Analyze protective put strategies for a stock position.

    Args:
        ticker: Stock ticker (e.g. 'TQQQ')
        num_shares: Number of shares to protect (e.g. 5000)
        num_contracts: Number of put contracts to buy (e.g. 50)
        stock_price: Current stock price. If None, fetched via daily aggs.
        expiration_date: Expiration date to analyze. If None, uses nearest.
        strike_range_pct: Only fetch puts within this % of stock price (default 20).

    Returns a dict with strategies sorted by max_total_loss.
    """
    if client is None:
        client = get_polygon_client()

    # Get stock price first so we can filter strikes
    if stock_price is None:
        stock_price = _get_stock_price(client, ticker)
        if stock_price is None:
            logging.warning(f"Could not determine stock price for {ticker}")
            return None

    logging.info(f"{ticker} stock price: ${stock_price:.2f}")

    if expiration_date is None:
        expiration_date = get_nearest_expiration(ticker, client=client)
        if not expiration_date:
            logging.warning(f"No expiration dates found for {ticker}")
            return None

    # Filter strikes to a reasonable range around current price
    strike_lo = stock_price * (1 - strike_range_pct / 100)
    strike_hi = stock_price * (1 + strike_range_pct / 100)

    chain = get_options_chain(
        ticker,
        expiration_date=expiration_date,
        contract_type="put",
        strike_price_gte=strike_lo,
        strike_price_lte=strike_hi,
        client=client,
    )

    if not chain:
        logging.warning(f"No put options found for {ticker} exp={expiration_date}")
        return None

    position_value = stock_price * num_shares
    shares_per_contract = 100

    strategies = []
    for put in chain:
        strike = put["strike"]
        if strike is None:
            continue

        premium = put["last_close"]
        if not premium or premium <= 0:
            continue

        premium_total = premium * num_contracts * shares_per_contract
        max_loss_per_share = (stock_price + premium) - strike
        max_total_loss = max_loss_per_share * num_shares
        break_even = stock_price + premium
        downside_protected_pct = ((stock_price - strike) / stock_price) * 100

        strategies.append(
            {
                "strike": strike,
                "last_close": premium,
                "premium_total": premium_total,
                "max_loss_per_share": max_loss_per_share,
                "max_total_loss": max_total_loss,
                "downside_protected_pct": downside_protected_pct,
                "break_even": break_even,
                "volume": put["volume"],
                "contract_ticker": put["contract_ticker"],
            }
        )

    strategies.sort(key=lambda s: s["max_total_loss"])

    result = {
        "ticker": ticker,
        "stock_price": stock_price,
        "expiration_date": expiration_date,
        "position_value": position_value,
        "num_shares": num_shares,
        "num_contracts": num_contracts,
        "strategies": strategies,
    }

    logging.info(
        f"Protective put analysis for {ticker}: price=${stock_price:.2f}, "
        f"{len(strategies)} strikes, exp={expiration_date}"
    )
    return result


def print_protective_put_analysis(analysis):
    """Pretty-print the result of analyze_protective_puts."""
    if not analysis:
        print("No analysis available.")
        return

    ticker = analysis["ticker"]
    price = analysis["stock_price"]
    exp = analysis["expiration_date"]
    pos_val = analysis["position_value"]
    n_shares = analysis["num_shares"]
    n_contracts = analysis["num_contracts"]

    print(f"\n{'=' * 105}")
    print(f"Protective Put Analysis: {ticker}")
    print(
        f"Stock Price: ${price:.2f} | Shares: {n_shares:,} | "
        f"Contracts: {n_contracts} | Position: ${pos_val:,.0f}"
    )
    print(f"Expiration: {exp}")
    print(f"{'=' * 105}\n")

    header = (
        f"{'Strike':>8}  {'LastClose':>9}  {'Premium':>10}  "
        f"{'MaxLoss/sh':>10}  {'MaxLoss Tot':>12}  "
        f"{'Downside%':>9}  {'BreakEven':>9}  {'Volume':>8}"
    )
    print(header)
    print("-" * len(header))

    for s in analysis["strategies"]:
        print(
            f"${s['strike']:>7.2f}  "
            f"${s['last_close']:>8.2f}  "
            f"${s['premium_total']:>9,.0f}  "
            f"${s['max_loss_per_share']:>9.2f}  "
            f"${s['max_total_loss']:>11,.0f}  "
            f"{s['downside_protected_pct']:>8.1f}%  "
            f"${s['break_even']:>8.2f}  "
            f"{s['volume']:>8,}"
        )

    print()
