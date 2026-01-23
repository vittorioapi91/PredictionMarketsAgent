"""Main entry point for collecting Polymarket data"""

from src.polymarket import PolymarketClient, MarketDataProcessor


def main():
    """Collect all Polymarket data and save to CSV"""
    client = PolymarketClient()
    processor = MarketDataProcessor()

    markets = client.fetch_all_markets()
    if markets:
        csv_file = processor.get_output_path("raw_data")
        processor.save_markets_to_csv(markets, csv_file)


if __name__ == "__main__":
    main()
