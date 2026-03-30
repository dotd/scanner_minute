from ScannerMinute.src.option_utils import (
    analyze_protective_puts,
    print_protective_put_analysis,
)

if __name__ == "__main__":
    analysis = analyze_protective_puts(
        ticker="TQQQ",
        num_shares=5000,
        num_contracts=50,
    )
    print_protective_put_analysis(analysis)
