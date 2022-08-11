import pytest
from feature_factory.demo.channel_demo_store import Store
from feature_factory.demo.channel_demo_web import Web
from feature_factory.demo.channel_demo_catalog import Catalog

data_base_dir = "tests/data"

#
# Please read up on why PyTest is a better best practice in comparison with traditional unittest test case approach:
# https://docs.pytest.org/en/6.2.x/fixture.html
#


@pytest.fixture
def sales(spark_session):
    sales_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_store_sales_enhanced.csv", inferSchema=True, header=True)
    sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_store_sales_enhanced")


@pytest.fixture
def store(spark_session):
    store_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_store.csv", inferSchema=True, header=True)
    store_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_store")


@pytest.fixture
def web_sales(spark_session):
    web_sales_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_web_sales_enhanced.csv", inferSchema=True, header=True)
    web_sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_web_sales_enhanced")


@pytest.fixture
def item(spark_session):
    item_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_item.csv", inferSchema=True, header=True)
    item_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_item")


@pytest.fixture
def inventory(spark_session):
    inventory_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_inventory.csv", inferSchema=True, header=True)
    inventory_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_inventory")


@pytest.fixture
def date_dim(spark_session):
    datedim_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_date_dim.csv", inferSchema=True, header=True)
    datedim_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_date_dim")


@pytest.fixture
def catalog_sales(spark_session):
    cat_sales_df = spark_session.read.csv(f"{data_base_dir}/tomes_tpcds_delta_1tb_catalog_sales_enhanced.csv", inferSchema=True, header=True)
    cat_sales_df.createOrReplaceTempView("tomes_tpcds_delta_1tb_catalog_sales_enhanced")


def test_store(sales, store, item, inventory):
    store = Store(_snapshot_date="2002-01-31")
    print(store.config.configs)
    assert len(store.sources) == 3
    multipliable, base = store.Sales().get_all()
    assert len(multipliable.features) > 0
    assert store.get_data("sources.store")
    assert store.get_data("sources.store_sales")
    assert store.get_data('sources.item')


def test_web_sales(web_sales, item, inventory):
    web = Web(_snapshot_date="2002-01-31")
    print(web.config.configs)
    assert len(web.sources) == 1
    multipliable, base = web.Sales().get_all()
    assert len(multipliable.features) > 0


def test_catalog_sales(catalog_sales, item, inventory):
    catalog = Catalog(_snapshot_date="2002-01-31")
    print(catalog.config.configs)
    assert len(catalog.sources) > 0
    multipliable, base = catalog.Sales().get_all()
    assert len(multipliable.features) > 0


def test_trend(store):
    store = Store(_snapshot_date="2002-01-31")
    mult_features, _ = store.Sales().get_all()
    trends = store.Trends(mult_features, [["1w", "12w"], ["1m", "12m"]]).get_all()
    assert len(trends.features) > 0
