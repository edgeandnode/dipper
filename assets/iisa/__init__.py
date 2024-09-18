"""
The Indexing Indexer Selection Algorithm (IISA) module.
"""

from .bq import BigQueryProvider
from .data_manager import DataManager
from .geoip import GeoipResolver
from .iisa import DataProcessor
from .network import NetworkProvider

__all__ = [
    "DataManager",
    "DataProcessor",
    "BigQueryProvider",
    "GeoipResolver",
    "NetworkProvider",
]

# This block serves as a functional test and an example implementation
if __name__ == "__main__":
    import logging

    # Setup basic logging
    logging.basicConfig(level=logging.INFO)

    def process_subgraph(
        data,
        subgraph_id,
        prices,
        existing_agreements,
        pending_agreements,
        blacklist,
        weights=None,
        bigquery=BigQueryProvider("graph-mainnet", "US"),
    ):
        return DataProcessor(
            data=data,
            subgraph_id=subgraph_id,
            prices=prices,
            existing_agreements=existing_agreements,
            pending_agreements=pending_agreements,
            blacklist=blacklist,
            bigquery=bigquery,
            weights=weights
            or {
                "lat_lin_reg_coefficient": 0.2424,
                "uptime_score": 0.1667,
                "existing_dips_agreements": 0.1212,
                "stake_to_fees_iqr_deviation": 0.1023,
                "success_rate": 0.0625,
                "avg_sync_duration": 0.0625,
                "indexing_agreement_acceptance_latency": 0.2424,
            },
        )

    try:
        # Initialize DataManager (done once at project creation)
        # This also performs the initial data fetch
        geoip = GeoipResolver()
        network_provider = NetworkProvider(geoip=geoip)
        bigquery_provider = BigQueryProvider("graph-mainnet", "US")
        data_manager = DataManager(bigquery=bigquery_provider, network=network_provider)

        # Simulate periodic data update (should be done once every 24 hours)
        data_manager.fetch_data_and_update()

        # Get the latest data
        data = data_manager.get_data()
        if data is None:
            raise ValueError("DataManager initial fetch required")

        # Save the data to a CSV file
        data.to_csv("DataManager_GetData_DataFrame.csv", index=False)

        # Example values
        subgraph_id = "QmSubgraph1"
        prices = {"0xIndexer1": 10, "0xIndexer2": 20, "0xIndexer3": 15}
        existing_agreements = {
            "0xIndexer1": ["QmSubgraph1", "QmSubgraph2"],
            "0xIndexer2": ["QmSubgraph3"],
        }
        pending_agreements = {"0xIndexer3": ["QmSubgraph4"]}
        blacklist = ["0xBlacklistedIndexer"]

        # Process subgraph
        try:
            processor = process_subgraph(
                data,
                subgraph_id,
                prices,
                existing_agreements,
                pending_agreements,
                blacklist,
            )
            added, cancelled = processor.get_indexer_selections()
            print(f"Initial processing - Added: {added}, Cancelled: {cancelled}")

        except Exception as e:
            print(f"An error occurred during new subgraph processing: {e}")

        # Simulate updates with new data:
        new_subgraph_id = "QmNewSubgraphId"
        new_prices = {
            **prices,
            "0xIndexer2": 22,  # Update existing price
            "0xIndexer4": 25,  # Add new price
        }
        new_existing_agreements = {
            **existing_agreements,
            "0xNewIndexer": ["QmNewSubgraph"],  # Add a new agreement
            "0xIndexer1": existing_agreements["0xIndexer1"]
            + ["QmNewSubgraph2"],  # Update existing
        }
        new_pending_agreements = {
            **pending_agreements,
            "0xIndexer4": ["QmSubgraph5"],  # Add a new pending agreement
        }
        new_blacklist = ["0xBlacklistedIndexer", "0xNewBlacklistedIndexer"]
        new_weights = {
            "lat_lin_reg_coefficient": 0.1,
            "uptime_score": 0.1,
            "existing_dips_agreements": 0.1,
            "stake_to_fees_iqr_deviation": 0.1,
            "success_rate": 0.1,
            "avg_sync_duration": 0.1,
            "indexing_agreement_acceptance_latency": 0.4,
        }

        # Process new subgraph
        try:
            processor = process_subgraph(
                data,
                new_subgraph_id,
                new_prices,
                new_existing_agreements,
                new_pending_agreements,
                new_blacklist,
                weights=new_weights,
            )
            added, cancelled = processor.get_indexer_selections()
            print(f"New subgraph processing - Added: {added}, Cancelled: {cancelled}")

        except Exception as e:
            print(f"An error occurred during new subgraph processing: {e}")

        # Demonstrate blackisting indexers.
        updated_blacklist = [
            "0xBlacklistedIndexer",
            "0xNewBlacklistedIndexer",
            "0xAnotherBlacklistedIndexer",
            "0xIndexer1",
            "0xIndexer2",
            "0xNewIndexer",
        ]

        # Create a DataProcessor instance
        try:
            data_processor = DataProcessor(
                data,
                new_subgraph_id,
                new_prices,
                existing_agreements=new_existing_agreements,
                pending_agreements=new_pending_agreements,
                blacklist=new_blacklist,
                weights=new_weights,
                bigquery=BigQueryProvider("graph-mainnet", "US"),
            )

            # Update and reprocess data
            cancelled_agreements = (
                data_processor.update_blacklist_cancel_indexing_agreements(
                    updated_blacklist
                )
            )

            print(
                f"After update_blacklist_cancel_indexing_agreements - Cancelled: {cancelled_agreements}"
            )

        except Exception as e:
            print(f"An error occurred during creation of DataProcessor instance: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
