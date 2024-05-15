# Hudi on Databricks

If you're a Databricks user and are wondering `How do I run Apache Hudi on Databricks?`, you’re not alone. In this blog, we'll walk you through the straightforward process of setting up and configuring Hudi within your Databricks environment. All you need is your Databricks account, and you're ready to follow along. We'll cover everything from creating compute instances to installing the necessary libraries, ensuring you're set up for success.
Once configured, we'll explore the essential configurations needed to leverage Hudi tables effectively. With these insights, you'll be equipped to tackle data management tasks with ease, all within the familiar Databricks environment.

## Pre-requisites:
- A Databricks account. Note that you can also work with Databricks community edition to follow along with this blog.

## Setup:
1. From your `Databricks` console, click `Compute` and `Create Compute`.
2. Choose a relevant compute name and choose a runtime version that is compatible with the Hudi version you are planning to use. For example, Databricks’ 13.3 runtime supports Spark 3.4.1 which is currently supported by Hudi.

   <img width="729" alt="Screenshot 2024-05-15 at 10 19 49 AM" src="https://github.com/sagarlakshmipathy/hudi-on-databricks/assets/30472234/319e28b5-ef40-488f-bbfc-a3752129257f">


4. Once the compute is created, head into the `Libraries` tab inside `hudi-on-databricks` and click on `Install new`.
5. In the pop-up, choose `Maven` and provide the Hudi-Spark maven coordinates.
   
   <img width="684" alt="Screenshot 2024-05-14 at 4 44 46 PM" src="https://github.com/sagarlakshmipathy/hudi-on-databricks/assets/30472234/251ccfa3-d1b1-41fd-a93e-65a835405176">


The minimal configurations to work with Hudi tables with spark needs to be provided to the Databricks runtime environment. This can be written to the `Spark` tab under `Configuration` inside `hudi-on-databricks`. Paste the following configurations:

```properties
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.catalog.spark_catalog org.apache.spark.sql.hudi.catalog.HoodieCatalog
spark.sql.extensions org.apache.spark.sql.hudi.HoodieSparkSessionExtension
spark.kryo.registrator org.apache.spark.HoodieSparkKryoRegistrar
```

Now you have to Confirm the changes which restarts the compute and as a result any notebook which uses this compute will be able to read and write hudi tables.

## Putting it all together:
Let's create a notebook based on the example provided in the Hudi-Spark quickstart page. You can also download the notebook from this repository and upload it to Databricks for ease-of-use.

As we have already updated the compute with the Hudi-Spark bundled jar, it’s automatically added to the classpath and you can start reading and writing Hudi tables as you would in any other Spark environment.

```python
# pyspark
from pyspark.sql.functions import lit, col

tableName = "trips_table"
basePath = "file:///tmp/trips_table"
```
```python
columns = ["ts","uuid","rider","driver","fare","city"]
data =[(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
      (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
      (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
      (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
      (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")]
inserts = spark.createDataFrame(data).toDF(*columns)

inserts.show()
```

```python
hudi_options = {
   'hoodie.table.name': tableName
}

inserts.write.format("hudi"). \
   options(**hudi_options). \
   mode("overwrite"). \
   save(basePath)
```


> Note: Pay close attention to hoodie.file.index.enable being set to false, this enables the use of spark file index implementation for Hudi, that speeds up listing of large tables and is mandatory if you're using Databricks to read Hudi tables.

```python
tripsDF = spark.read.format("hudi").option("hoodie.file.index.enable", "false").load(basePath)

tripsDF.show()
```

You can also turn this into a view to run SQL queries from the same notebook.

```python
tripsDF.createOrReplaceTempView("trips_table")
```

```python
%sql
SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0
```

<img width="1082" alt="Screenshot 2024-05-15 at 10 18 46 AM" src="https://github.com/sagarlakshmipathy/hudi-on-databricks/assets/30472234/8597444d-e1ca-4f9a-a7ee-40be7bcc3a44">


## Conclusion
In conclusion, integrating Apache Hudi into your Databricks workflow offers a streamlined approach to data lake management and processing. By following the steps outlined in this blog, you've learned how to seamlessly configure Hudi within your Databricks environment, empowering you to handle data operations with efficiency and confidence.
With Hudi, you can leverage incremental data processing, simplified data ingestion, and efficient data management – all within the familiar Databricks ecosystem. Whether you're working with heavy workloads or complex data structures, Hudi on Databricks provides the tools you need to succeed.
We hope this guide has equipped you with the knowledge and skills to unlock the full potential of Apache Hudi in your data projects. By harnessing the power of Hudi on Databricks, you can streamline your data operations, drive insights, and accelerate innovation.
