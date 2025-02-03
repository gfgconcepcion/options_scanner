import options_scanner as osc
import os

def main():
    equity_exchange = "nasdaq"
    equity_ticker = "meta"
    aggregate_options_chain_data = osc.get_and_save_aggregate_options_chain(equity_exchange, equity_ticker)
    earliest_expiring_contracts_data = osc.get_and_save_earliest_expiring_contracts(aggregate_options_chain_data)


if __name__ == '__main__':
    main()