import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from ScannerMinute.src.rocksdict_utils import read_bars
from ScannerMinute.src.polygon_utils import COLUMNS


# Bar indices derived from COLUMNS
_COL_INDEX = {name: i for i, name in enumerate(COLUMNS)}
IDX_TICKER = _COL_INDEX["ticker"]
IDX_DATETIME = _COL_INDEX["datetime_utc"]
IDX_OPEN = _COL_INDEX["open"]
IDX_HIGH = _COL_INDEX["high"]
IDX_LOW = _COL_INDEX["low"]
IDX_CLOSE = _COL_INDEX["close"]
IDX_VOLUME = _COL_INDEX["volume"]
IDX_VWAP = _COL_INDEX["vwap"]
IDX_TIMESTAMP = _COL_INDEX["timestamp"]
IDX_TRANSACTIONS = _COL_INDEX["transactions"]
IDX_OTC = _COL_INDEX["otc"]


@dataclass
class Order:
    """A buy or sell order issued by a Strategy."""
    ticker: str
    action: str  # "buy" or "sell"
    amount: float  # dollar amount to buy, or number of shares to sell


@dataclass
class Position:
    """Shares held for a single ticker."""
    shares: float = 0.0
    avg_cost: float = 0.0


@dataclass
class Portfolio:
    """Tracks cash, positions, and trade history."""
    cash: float = 1000.0
    positions: dict = field(default_factory=dict)  # ticker -> Position
    trade_log: list = field(default_factory=list)

    def total_value(self, current_prices: dict) -> float:
        """Total portfolio value = cash + sum of positions * current price."""
        value = self.cash
        for ticker, pos in self.positions.items():
            if ticker in current_prices and pos.shares > 0:
                value += pos.shares * current_prices[ticker]
        return value

    def execute_buy(self, ticker, dollar_amount, exec_price, timestamp):
        if dollar_amount <= 0 or exec_price <= 0:
            return
        spend = min(dollar_amount, self.cash)
        if spend <= 0:
            logging.warning(f"[PORTFOLIO] No cash to buy {ticker} (cash={self.cash:.2f})")
            return
        shares = spend / exec_price
        self.cash -= spend

        if ticker not in self.positions:
            self.positions[ticker] = Position()
        pos = self.positions[ticker]
        total_cost = pos.avg_cost * pos.shares + spend
        pos.shares += shares
        pos.avg_cost = total_cost / pos.shares if pos.shares > 0 else 0.0

        self.trade_log.append({
            "timestamp": timestamp,
            "action": "buy",
            "ticker": ticker,
            "shares": shares,
            "price": exec_price,
            "amount": spend,
            "cash_after": self.cash,
        })
        logging.debug(
            f"[PORTFOLIO] BUY {shares:.4f} {ticker} @ {exec_price:.2f} = ${spend:.2f} | cash={self.cash:.2f}"
        )

    def execute_sell(self, ticker, shares_to_sell, exec_price, timestamp):
        if shares_to_sell <= 0 or exec_price <= 0:
            return
        if ticker not in self.positions or self.positions[ticker].shares <= 0:
            logging.warning(f"[PORTFOLIO] No position in {ticker} to sell")
            return
        pos = self.positions[ticker]
        actual_shares = min(shares_to_sell, pos.shares)
        proceeds = actual_shares * exec_price
        pos.shares -= actual_shares
        self.cash += proceeds

        self.trade_log.append({
            "timestamp": timestamp,
            "action": "sell",
            "ticker": ticker,
            "shares": actual_shares,
            "price": exec_price,
            "amount": proceeds,
            "cash_after": self.cash,
        })
        logging.debug(
            f"[PORTFOLIO] SELL {actual_shares:.4f} {ticker} @ {exec_price:.2f} = ${proceeds:.2f} | cash={self.cash:.2f}"
        )


class Strategy(ABC):
    """
    Abstract base class for trading strategies.

    Subclass and implement on_bar() to define trading logic.
    The strategy receives market data and returns a list of Orders.
    """

    def __init__(self):
        self.memory = {}

    @abstractmethod
    def on_bar(self, timestamp, bars_by_ticker: dict, portfolio: Portfolio) -> list[Order]:
        """
        Called once per cycle with the current bar for each ticker.

        Args:
            timestamp: current bar timestamp (epoch ms)
            bars_by_ticker: dict of {ticker: bar_list} for the current candle
            portfolio: current Portfolio state (read-only recommended)

        Returns:
            list of Order objects to execute at the next candle's average price.
        """
        pass


def _bar_avg_price(bar) -> float:
    """Average price of a bar: (open + high + low + close) / 4."""
    return (bar[IDX_OPEN] + bar[IDX_HIGH] + bar[IDX_LOW] + bar[IDX_CLOSE]) / 4


class BackTesting:
    """
    Replays historical bar data and runs a Strategy against it.

    The data is loaded from RocksDB and played sequentially.
    Orders from the Strategy are executed at the next candle's average price.
    """

    def __init__(
        self,
        tickers: list[str],
        date_start: str,
        date_end: str,
        timespan: str = "minute",
        db_path: str = None,
        initial_cash: float = 1000.0,
    ):
        self.tickers = tickers
        self.date_start = date_start
        self.date_end = date_end
        self.timespan = timespan
        self.db_path = db_path
        self.initial_cash = initial_cash

        # Load all bars from DB
        self._bars_by_ticker = {}
        self._all_timestamps = set()
        self._load_data()

        # Sorted unique timestamps across all tickers
        self._timeline = sorted(self._all_timestamps)

    def _load_data(self):
        from ScannerMinute.definitions import PROJECT_ROOT_DIR

        db_path = self.db_path or f"{PROJECT_ROOT_DIR}/data/rocksdict/"
        all_bars = read_bars(
            db_path,
            self.timespan,
            self.tickers,
            f"{self.date_start}T00:00:00",
            f"{self.date_end}T23:59:59",
        )
        for bar in all_bars:
            ticker = bar[IDX_TICKER]
            ts = bar[IDX_TIMESTAMP]
            if ticker not in self._bars_by_ticker:
                self._bars_by_ticker[ticker] = {}
            self._bars_by_ticker[ticker][ts] = bar
            self._all_timestamps.add(ts)

        logging.info(
            f"[BACKTEST] Loaded {len(all_bars)} bars for {len(self._bars_by_ticker)} tickers, "
            f"{len(self._timeline)} unique timestamps"
        )

    def run(self, strategy: Strategy) -> dict:
        """
        Run the backtest with the given Strategy.

        Returns a dict with:
            portfolio: final Portfolio
            timeline: list of (timestamp, portfolio_value) snapshots
            trade_count: total number of trades executed
        """
        portfolio = Portfolio(cash=self.initial_cash)
        pending_orders = []
        value_timeline = []

        for i, ts in enumerate(self._timeline):
            # Build current bars for this timestamp
            bars_this_cycle = {}
            current_prices = {}
            for ticker in self.tickers:
                bar = self._bars_by_ticker.get(ticker, {}).get(ts)
                if bar is not None:
                    bars_this_cycle[ticker] = bar
                    current_prices[ticker] = bar[IDX_CLOSE]

            # Execute pending orders from previous cycle at this candle's avg price
            for order in pending_orders:
                if order.ticker not in bars_this_cycle:
                    logging.debug(f"[BACKTEST] No bar for {order.ticker} at {ts}, skipping order")
                    continue
                exec_price = _bar_avg_price(bars_this_cycle[order.ticker])
                if order.action == "buy":
                    portfolio.execute_buy(order.ticker, order.amount, exec_price, ts)
                elif order.action == "sell":
                    portfolio.execute_sell(order.ticker, order.amount, exec_price, ts)

            # Record portfolio value
            value_timeline.append((ts, portfolio.total_value(current_prices)))

            # Ask strategy for new orders (to be executed next cycle)
            pending_orders = strategy.on_bar(ts, bars_this_cycle, portfolio)
            if pending_orders is None:
                pending_orders = []

        # Final summary
        final_value = value_timeline[-1][1] if value_timeline else self.initial_cash
        pnl = final_value - self.initial_cash
        pnl_pct = (pnl / self.initial_cash) * 100

        logging.info(f"[BACKTEST] {'=' * 60}")
        logging.info(f"[BACKTEST] Period:       {self.date_start} to {self.date_end}")
        logging.info(f"[BACKTEST] Timespan:     {self.timespan}")
        logging.info(f"[BACKTEST] Tickers:      {len(self.tickers)}")
        logging.info(f"[BACKTEST] Bars played:  {len(self._timeline)}")
        logging.info(f"[BACKTEST] Trades:       {len(portfolio.trade_log)}")
        logging.info(f"[BACKTEST] Initial:      ${self.initial_cash:,.2f}")
        logging.info(f"[BACKTEST] Final:        ${final_value:,.2f}")
        logging.info(f"[BACKTEST] P&L:          ${pnl:,.2f} ({pnl_pct:+.2f}%)")
        logging.info(f"[BACKTEST] Cash:         ${portfolio.cash:,.2f}")
        for ticker, pos in portfolio.positions.items():
            if pos.shares > 0:
                price = current_prices.get(ticker, 0)
                logging.info(
                    f"[BACKTEST]   {ticker}: {pos.shares:.4f} shares @ avg ${pos.avg_cost:.2f} "
                    f"(current ${price:.2f}, value ${pos.shares * price:,.2f})"
                )
        logging.info(f"[BACKTEST] {'=' * 60}")

        return {
            "portfolio": portfolio,
            "value_timeline": value_timeline,
            "trade_count": len(portfolio.trade_log),
            "initial_cash": self.initial_cash,
            "final_value": final_value,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
        }
