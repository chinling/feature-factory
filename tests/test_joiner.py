import pytest
import unittest
from feature_factory.framework.feature_factory import Helpers
from feature_factory.framework.feature_factory.data import DataSrc, Joiner


class TestJoiner(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_fixture(self, spark_session):
        self.spark = spark_session

    def setUp(self):
        self.helpers = Helpers()

    def test__joiners(self):
        trans_df = self.spark.createDataFrame([(1, 100, "item1"), (2, 200, "item2")], ["trans_id", "sales", "item_id"])
        dim_df = self.spark.createDataFrame([("item1", "desc1"), ("item2", "desc2")], ["item_id", "item_desc"])
        joiner = Joiner(dim_df, on=["item_id"], how="inner")
        src_df = DataSrc(trans_df, joiners=[joiner]).to_df()
        assert len(src_df.columns) == 3
