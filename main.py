import options_scanner as osc

def main():
    equity_exchange = 'nasdaq'
    equity_ticker = 'meta'
    # aggregate_options_chain_data = osc.get_and_save_aggregate_options_chain(equity_exchange, equity_ticker)
    # earliest_expiring_contracts_data = osc.get_and_save_earliest_expiring_contracts(aggregate_options_chain_data)
    # most_recent_equity_price = osc.get_most_recent_equity_price(equity_exchange, equity_ticker))
    # price_history = osc.get_equity_price_history(equity_exchange, equity_ticker)
    # df = osc.calculate_price_differences(price_history)
    # osc.see_data_structure(equity_exchange, equity_ticker)


if __name__ == '__main__':
    main()