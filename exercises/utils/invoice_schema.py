from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

schema = StructType(
    [
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField(
            "DeliveryAddress",
            StructType(
                [
                    StructField("AddressLine", StringType()),
                    StructField("City", StringType()),
                    StructField("State", StringType()),
                    StructField("PinCode", StringType()),
                    StructField("ContactNumber", StringType()),
                ]
            ),
        ),
        StructField(
            "InvoiceLineItems",
            ArrayType(
                StructType(
                    [
                        StructField("ItemCode", StringType()),
                        StructField("ItemDescription", StringType()),
                        StructField("ItemPrice", DoubleType()),
                        StructField("ItemQty", IntegerType()),
                        StructField("TotalValue", DoubleType()),
                    ]
                )
            ),
        ),
    ]
)
