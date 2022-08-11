import pytest
import unittest
from feature_factory.framework.feature_factory.feature import Feature, FeatureSet
from feature_factory.framework.feature_factory.feature_dict import ImmutableDictBase
from feature_factory.framework.feature_factory import Feature_Factory
from feature_factory.framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType


class CommonFeatures(ImmutableDictBase):
    def __init__(self):
        self._dct["customer_id"] = Feature(_name="customer_id", _base_col=f.col("ss_customer_sk"))
        self._dct["trans_id"] = Feature(_name="trans_id", _base_col=f.concat("ss_ticket_number","d_date"))

    @property
    def collector(self):
        return self._dct["customer_id"]

    @property
    def trans_id(self):
        return self._dct["trans_id"]


class Filters(ImmutableDictBase):
    def __init__(self):
        self._dct["valid_sales"] = f.col("ss_net_paid") > 0

    @property
    def valid_sales(self):
        return self._dct["valid_sales"]


class StoreSales(CommonFeatures, Filters):
    def __init__(self):
        self._dct = dict()
        CommonFeatures.__init__(self)
        Filters.__init__(self)

        self._dct["total_trans"] = Feature(_name="total_trans",
                                           _base_col=self.trans_id,
                                           _filter=[],
                                           _negative_value=None,
                                           _agg_func=f.countDistinct)

        self._dct["total_sales"] = Feature(_name="total_sales",
                                           _base_col=f.col("ss_net_paid").cast("float"),
                                           _filter=self.valid_sales,
                                           _negative_value=0,
                                           _agg_func=f.sum)

    @property
    def total_sales(self):
        return self._dct["total_sales"]

    @property
    def total_trans(self):
        return self._dct["total_trans"]


@pytest.fixture
def sales(spark_session):
    with open("tests/data/sales_store_schema.json") as f:
        sales_schema = StructType.fromJson(json.load(f))
        sales_df = spark_session.read.csv("tests/data/sales_store_tpcds.csv", schema=sales_schema, header=True)
        return sales_df


def test_feature_dict(sales):
    helpers = Helpers()
    multiplier = helpers.get_categoricals_multiplier(sales, ["i_category"])
    features = StoreSales()
    fs = FeatureSet()
    fs.add_feature(features.total_sales)
    cats_fs = fs.multiply(multiplier, "")
    ff = Feature_Factory()
    df = ff.append_features(sales, [features.collector], [cats_fs])
    assert df.count() == sales.select("ss_customer_sk").distinct().count()
