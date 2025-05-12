from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def columnPrefixRemove(dataframe,prefix_raw):
    '''
    This function takes dataframe and col prefix to normalize the column names and returns a new df
    '''
    prefix = prefix_raw + '_'
    columns = dataframe.columns
    renamed_cols = [i[len(prefix):]for i in columns if i.startswith(prefix)]
    return dataframe.toDF(*renamed_cols)
    # we dont need to register this function as Spark UDF as this a dataframe level transformation and not a row and column wise transformations

if __name__ == '__main__':

    spark = (
        SparkSession.builder
                    .appName('tpchFlattenedView')
                    .getOrCreate()
    )

    # Loading required datasets
    
    customer_df = (
        spark.read.table('samples.tpch.customer')
    )

    order_df = (
        spark.read.table('samples.tpch.orders')
    )

    lineitem_df = (
        spark.read.table('samples.tpch.lineitem')
    )

    part_df = (
        spark.read.table('samples.tpch.part')
    )

    supplier_df = (
        spark.read.table('samples.tpch.supplier')
    )

    region_df = (
        spark.read.table('samples.tpch.region')
    )

    nation_df = (
        spark.read.table('samples.tpch.nation')
    )

    # Since all the columns in the dataframes has a prefix of the table name we would want to write a function to remove those
    
    customerDF = columnPrefixRemove(customer_df, 'c')
    orderDF = columnPrefixRemove(order_df, 'o')
    lineitemDF = columnPrefixRemove(lineitem_df, 'l')
    partDF = columnPrefixRemove(part_df, 'p')
    supplierDF = columnPrefixRemove(supplier_df, 's')
    regionDF = columnPrefixRemove(region_df, 'r')
    nationDF = columnPrefixRemove(nation_df, 'n')

    #########################################################################################################################################################
    # CREATING CONSOLIDATED COUNTRY_DF FROM REGION AND NATION
    #########################################################################################################################################################

    # Assign aliases to DataFrames as we have same column names in both the dataframes
    aliasnationDF = nationDF.alias("nation")
    aliasregionDF = regionDF.alias("region")

    join_expr = aliasnationDF.regionkey == aliasregionDF.regionkey
    join_type = 'inner'

    countryDF = aliasnationDF.join(
        broadcast(aliasregionDF),
        join_expr,
        join_type
    ).select(
        col("nation.nationkey"),               
        col("region.regionkey").alias("regionKey"),  
        col("nation.name").alias("nationname"),      
        col("region.name").alias("regionname")      
    )

    #########################################################################################################################################################
    # STAGING FLATTENED VIEW
    #########################################################################################################################################################
    
    # repartitioning large tables to optimize join operation
    lineitemDF = lineitemDF.repartition("orderkey")
    orderDF = orderDF.repartition("orderkey")

    # aliasing for join ease
    aliascustomerDF = customerDF.alias('customer')
    aliasorderDF = orderDF.alias('order')
    aliaslineitemDF = lineitemDF.alias('lineitem')
    aliaspartDF = partDF.alias('part')
    aliassupplierDF = supplierDF.alias('supplier')
    aliascountryDF = countryDF.alias('country')

    # Calculate max year of orders to take only last 2 years of data for reporting purposes
    max_order_date = lineitemDF.agg(
        max(year(col('receiptdate'))).alias('max_year')
    ).collect()[0]['max_year']

    # Create stg1 dataset for all joining operations

    stg1_orders = aliaslineitemDF.filter(
        year(col('receiptdate')) == max_order_date - 2  # Focus on the last 2 years of data
    ).join(
        aliasorderDF,
        aliaslineitemDF.orderkey == aliasorderDF.orderkey,
        'inner'
    ).join(
        aliassupplierDF,
        aliaslineitemDF.suppkey == aliassupplierDF.suppkey,
        'right'
    ).join(
        aliaspartDF,
        aliaslineitemDF.partkey == aliaspartDF.partkey,
        'right'
    ).join(
        aliascustomerDF,
        aliasorderDF.custkey == aliascustomerDF.custkey,
        'right'
    ).join(
        broadcast(aliascountryDF),
        aliascustomerDF.nationkey == aliascountryDF.nationkey,
        'inner'
    ).select(
        col('customer.custkey').alias('customerKey'),
        col('customer.name').alias('customerName'),
        col('customer.nationkey').alias('customerNationKey'),
        col('country.nationname').alias('customerNation'),  # Customer nation name
        col('country.regionname').alias('customerRegion'),
        col('customer.mktsegment').alias('customerSegment'),
        col('part.partkey').alias('partKey'),
        col('part.name').alias('partName'),
        col('part.mfgr').alias('partManufacturer'),
        col('part.brand').alias('partBrand'),
        col('part.type').alias('partType'),  
        col('supplier.suppkey').alias('supplierKey'),
        col('supplier.name').alias('supplierName'),
        col('supplier.nationkey').alias('supplierNationKey'),
        col('order.orderkey').alias('orderKey'),
        col('order.orderdate').alias('orderDate'),
        col('lineitem.shipdate').alias('shipDate'),
        col('lineitem.commitdate').alias('expectedShipDate'),
        col('lineitem.receiptdate').alias('DeliveryDate'),
        col('order.orderpriority').alias('orderPriority'),
        col('order.orderstatus').alias('orderStatus'),
        col('order.shippriority').alias('shipPriority'),
        round(col('lineitem.quantity'), 2).alias('quantity'), 
        round(col('part.retailprice'), 2).alias('partRetailPrice'),
        round(col('lineitem.extendedprice'), 2).alias('extendedPrice'),
        round(col('lineitem.discount'), 2).alias('discountRate'),
        round(col('lineitem.tax'), 2).alias('taxRate'),
        col('lineitem.returnflag').alias('isReturned')
    )
    stg1_orders.persist()

    # Creating a stg2 dataset to join back with country df for supplier nation

    stg2_orders = stg1_orders.alias('staging').join(
        broadcast(aliascountryDF),
        stg1_orders.supplierNationKey == aliascountryDF.nationkey,
        'inner'
    ).select(
        'staging.*',
        col('country.nationname').alias('supplierNation'),
        col('country.regionname').alias('supplierRegion')
    )

    # Creating a stg3 dataset for all the metric calculations for analytics purposes
    # delayed shiping
    # delayed delivery based on order status taking hypothetical numbers
    # calculate accounting metrics 

    stg3_orders = stg2_orders.withColumn(
        'isDelayedShipping',
        when(
            col('shipDate') > col('expectedShipDate'),
            'true'
        ).otherwise(
            'false'
        )
    ).withColumn( # some hypothetical numbers to evaluate 
        'isDelayedDelivery',
        when(
            (col('orderPriority') == '1-URGENT') & (col('deliveryDate') > date_add(col('shipDate'),30)),
            True
        ).when(
            (col('orderPriority') == '2-HIGH') & (col('deliveryDate') > date_add(col('shipDate'),45)),
            True
        ).when(
            (col('orderPriority') == '3-MEDIUM') & (col('deliveryDate') > date_add(col('shipDate'),60)),
            True
        ).otherwise(
            False
        )
    ).withColumn(
        'discountAmount',
        col('extendedPrice')*col('discountRate')
    ).withColumn(
        'netSaleBeforeTax',
        col('extendedPrice') - col('discountAmount')
    ).withColumn(
        'taxAmount',
        col('netSaleBeforeTax')*col('taxRate')
    ).withColumn(
        'netSaleAfterTax',
        col('netSaleBeforeTax') - col('taxAmount')
    ).withColumn(
        'profit',
        col('netSaleAfterTax') - col('partRetailPrice')
    ).withColumn(
        'profitMarginPercent',
        (col('profit')/col('netSaleAfterTax')) * 100
    )
    stg3_orders.persist()
    
    #########################################################################################################################################################
    # VIZ LAYER FLATTENED VIEW
    #########################################################################################################################################################

    flattened_tpch = stg3_orders.select(
        'customerKey',
        'customerName',
        'customerNation',
        'customerRegion',
        'customerSegment',
        'partType',
        'partName',
        'supplierKey',
        'supplierNation',
        'supplierRegion',
        'orderKey',
        'orderPriority',
        'orderDate',
        'shipDate',
        'expectedShipDate',
        'isDelayedShipping',
        'deliveryDate',
        'isDelayedDelivery',
        'quantity',
        col('partRetailPrice').alias('unitPrice'),
        col('extendedPrice').alias('grossSale'),
        'discountAmount',
        'netSaleBeforeTax',
        'taxAmount',
        'netSaleAfterTax',
        'profit',
        'profitMarginPercent'
    )