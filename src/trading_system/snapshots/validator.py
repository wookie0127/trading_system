from src.trading_system.snapshots.schemas import MarketSnapshot


class SnapshotValidator:
    @staticmethod
    def validate(snapshot: MarketSnapshot) -> bool:
        """
        Validates completeness and freshness of a MarketSnapshot.
        Returns False if stale or incomplete (Fail-closed principle).
        """
        if not snapshot.data_quality.is_complete:
            return False
        if snapshot.data_quality.stale_sources:
            return False
        if snapshot.price.close <= 0:
            return False
        return True
