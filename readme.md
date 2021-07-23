

# Overview

## 说明

翻译自 https://spark.apache.org/docs/2.2.1/sql-programming-guide.html spark 2.2.1版本的spark-sql文档的中文翻译。

## Overview

```java


Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL 
ovide Spark with more information about the structure of both the data and the computation being performed.
   spark SQL是一个spark 模块，用于结构化数据的处理。不想基本的spark rdd api,这些由spark sql提供的接口给spark提供了关于数据和正在这行的计算的结构的信息。
   
Internally, Spark SQL uses this extra information to perform extra optimizations. There are several ways to interact with Spark SQL including SQL and the Dataset API. When computing a result the same execution engine is used,
从内部来说，spark sql使用这种额外信息来执行一些优化。有很多种方式和spark sql 交互，包括SQL和dataset api.当计算一个结果时， 会使用相同的执行引擎。

independent of which API/language you are using to express the computation. This unification means that developers can easily switch back and forth between different APIs based on which provides the most natural way to express a given transformation.
   它独立于你在使用的哪种语言和api来表达这个计算。 这种统一性以为这开发者可以轻易在不同API之间切换，这种切换给我们提供了最自然的方式来表达一个给定的变换。

All of the examples on this page use sample data included in the Spark distribution and can be run in the spark-shell, pyspark shell, or sparkR shell.
   这一页的所有例子使用包含于spark distribution中的样例数据，可以在spark-shell,pyspark-shell,或者spark-R shell运行。
   

```

## sql

```java
spark-sql的一个用处是执行sql查询。 Spark-sql也可用于从已有的hive中读取数据。对于如何配置这个特性，请参考  https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#hive-tables
当从另一个编程语言内进行运行sql时，这些结果会以一个Dataset或者DataFrame返回。
你也可以使用命令行或者ODBC/JDBC和sql接口交互。

```



## Datasets和DataFrames

```python
一个Dataset是一个分布式的数据集合。它是一个1.6版本添加的新接口，提供了RDDs的益处（强类型，使用强大的lambda函数的能力），以及sparksql的优化引擎的好处。 一个dataset可以从JVM对象构建，然后使用函数式变换进行操作（例如,map，flatMap,filter,etc）.Dataset API可以从scala，java中获取。Python不支持dataset api.但是由于python的动态特性。Dataset api的很多好处都已经可以获得了（例如，可以通过名称row.columnName获得一行的域）R语言也是这样。
一个DataFrame是一个组织成命名列（named columns）的Dataset, 它等价于关系型数据库中的表格，或者R、PYTHON里面的一个数据帧，但是有更好的优化。DataFrame可以从很多源进行构建： 结构化数据文件，hive的表格，外部数据库，已有RDD。 它的API可以从scala java python r 中获取。在scala和java中，它表示为一个行的数据集(Dataset ofRows）.
在scala api中，dataframe只是Dataset[Row]的别名。但是在JAVA api里面，用户需要使用Dataset<Row>来表示DataFrame。

通篇文档，我们将scala/java的Datasets of Rows称为DataFrames。

```



## 开始入门

### 起点 SparkSession

```java
这个spark中的所有函数的入口是SparkSession class. 使用SparkSession.builder来创建基本的SparkSession.


```

#以下是pyspark版本的代码，完整代码在pyspark库的examples/src/main/python/sql/目录下的basic.py文件

spark 2.0的SpakSession提供了对hive特征的内在支持，包括使用hiveQL写查询的能力，获取hive UDF,以及从hive 表格读取数据的能力。你不需要有一个已有的hive设置就可以使用这些特性。



#### python实现

```python
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

```



完整代码： baisc.py

```python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating basic Spark SQL features.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/basic.py
"""
# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

# $example on:schema_inferring$
from pyspark.sql import Row
# $example off:schema_inferring$

# $example on:programmatic_schema$
# Import data types
from pyspark.sql.types import StringType, StructType, StructField
# $example off:programmatic_schema$


def basic_df_example(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.json("examples/src/main/resources/people.json")
    # Displays the content of the DataFrame to stdout
    df.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:create_df$

    # $example on:untyped_ops$
    # spark, df are from the previous example
    # Print the schema in a tree format
    df.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # Select only the "name" column
    df.select("name").show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    # +-------+---------+
    # |   name|(age + 1)|
    # +-------+---------+
    # |Michael|     null|
    # |   Andy|       31|
    # | Justin|       20|
    # +-------+---------+

    # Select people older than 21
    df.filter(df['age'] > 21).show()
    # +---+----+
    # |age|name|
    # +---+----+
    # | 30|Andy|
    # +---+----+

    # Count people by age
    df.groupBy("age").count().show()
    # +----+-----+
    # | age|count|
    # +----+-----+
    # |  19|    1|
    # |null|    1|
    # |  30|    1|
    # +----+-----+
    # $example off:untyped_ops$

    # $example on:run_sql$
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:run_sql$

    # $example on:global_temp_view$
    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+

    # Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:global_temp_view$


def schema_inference_example(spark):
    # $example on:schema_inferring$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
    # Name: Justin
    # $example off:schema_inferring$


def programmatic_schema_example(spark):
    # $example on:programmatic_schema$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+
    # $example off:programmatic_schema$

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    basic_df_example(spark)
    schema_inference_example(spark)
    programmatic_schema_example(spark)

    spark.stop()

```



#### scala实现

```scala
//


import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession
  .builder()
  .appName("Java Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate();
```

### 创建DataFrames

```java
使用SparkSession,应用可以从一个已有RDD， hive表格，或者spark数据源创建DataFrame.
举例，如下代基于一个JSON文件创建DataFrame
    
    
```

### java实现

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");

// Displays the content of the DataFrame to stdout
df.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```



#### python实现

```python

# spark is an existing SparkSession
df = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

```

### 无类型数据集操作（DataFrame Operations)

数据帧提供一个明确类型的语言用于结构化数据操作在Scala java python R

之前提到，在spark 2.0，数据帧只是行的数据集在SCALA 和java api。这些操作也被称为无类型变幻，和有类型变换形成对比，并且伴随着强类型scala java数据集

我们包括了一些使用Dataset来处理结构化数据的基本案例：



#### python实现

```python
# 可以通过属性（df.age）或者索引（df['age'])来获取DataFrame的列。但是前者对于交互数据操作更方便，鼓励使用后面一个形式，它经得起考验，当列名称也是DataFrame类的属性时也不会崩溃。
# spark, df are from the previous example
# Print the schema in a tree format
df.printSchema()
# root
# |-- age: long (nullable = true)
# |-- name: string (nullable = true)

# Select only the "name" column
df.select("name").show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+

# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()
# +-------+---------+
# |   name|(age + 1)|
# +-------+---------+
# |Michael|     null|
# |   Andy|       31|
# | Justin|       20|
# +-------+---------+

# Select people older than 21
df.filter(df['age'] > 21).show()
# +---+----+
# |age|name|
# +---+----+
# | 30|Andy|
# +---+----+

# Count people by age
df.groupBy("age").count().show()
# +----+-----+
# | age|count|
# +----+-----+
# |  19|    1|
# |null|    1|
# |  30|    1|
# +----+-----+

#完整代码位于 "examples/src/main/python/sql/basic.py"  
#除了简单的列引用和表达式，DataFrame还有丰富的函数库，包括字符串操作，日期算术，通用数学操作等等。完整的列表在这儿--https://spark.apache.org/docs/2.2.1/api/python/pyspark.sql.html#module-pyspark.sql.functions

```

### 程序地运行sql查询

SparkSession的sql函数能让应用运行sql query(已编程方式)，返回一个DataFrame结果

#### python实现

```python
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
#完整代码："examples/src/main/python/sql/basic.py"
```

### 全局临时视图

临时视图是session范围内的，如果创建它的会话终止了，这个视图也会消失。如果你想要一个临时视图，并且它能够在所有会话间共享，存活知道应用终止，你可以创建一个全局临时视图。它会和一个系统保留的数据库global_temp绑定，我们必须使用它的名称来指定它，例如SELECT * FROM global_temp.view1

#### python实现

```python

# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

```

### 创建数据集

数据集和RDD类似，但是，它不是使用java 序列号或者Kryo，他们使用一种特殊编码来序列化对象用户处理或者在网络传输。 编码器和标准序列化都负责将对象变为字节，但是编码器是动态生成的代码，使用一个允许spark执行很多操作例如filtering,sorting,hashing而没有将字节反序列化回对象的格式。

#### java实现

省略

#### scala实现

```scala
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface
case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()
// +----+---+
// |name|age|
// +----+---+
// |Andy| 32|
// +----+---+

// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
val path = "examples/src/main/resources/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```



### 和RDD互操作

spark-sql支持2中不同的方法将已有RDD转换为Datasets，第一种使用反射来推断RDD的结构，这个反射方法导致更加简洁的代码前提是你已经知道了这个结构。

第二种创建Datasets的方法是通过编程接口，允许你创建一个结构，然后将它应用到已有RDD。虽然这个方法更加啰嗦，它允许你创建数据集当这个列以及类型是未知的知道运行的时候才知道。

### 使用反射推断schema

#### java实现

```java
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

// Create an RDD of Person objects from a text file
JavaRDD<Person> peopleRDD = spark.read()
  .textFile("examples/src/main/resources/people.txt")
  .javaRDD()
  .map(line -> {
    String[] parts = line.split(",");
    Person person = new Person();
    person.setName(parts[0]);
    person.setAge(Integer.parseInt(parts[1].trim()));
    return person;
  });

// Apply a schema to an RDD of JavaBeans to get a DataFrame
Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
// Register the DataFrame as a temporary view
peopleDF.createOrReplaceTempView("people");

// SQL statements can be run by using the sql methods provided by spark
Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

// The columns of a row in the result can be accessed by field index
Encoder<String> stringEncoder = Encoders.STRING();
Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
    stringEncoder);
teenagerNamesByIndexDF.show();
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+

// or by field name
Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
    (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
    stringEncoder);
teenagerNamesByFieldDF.show();
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+
```





### python实现



spark sql可以将Row对象的RDD转换为DataFrame, Rows通过传入键值对作为Row 的参数来构建，每个key就是他的类名，类型通过在数据集上采样而推断出来

```python
from pyspark.sql import Row

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
# Name: Justin
```

### 



### 编程指定结构

当一个字典的键值对参数不能够提前确定（例如记录的结构编码在一个字符串里面，或者一个文本数据集去解析，字段会被不同用户映射为不同的名称），DataFrame可以使用以下三步进行创建：

1 从已有原始RDD创建元组或者列表RDD



2  创建StrucType和元组或者列表的结构匹配。

3 通过createDataFrame方法（SparkSession提供）将结构应用到RDD



例如

```python
# Import data types
from pyspark.sql.types import *

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

results.show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+
```

### 聚合

内置DataFrame函数提供了常用聚合函数例如count(), countDistinct(),avg() max() min() 等等。这些方法被设计用于DataFrames，但是sparksql有一些类型安全的函数版本在scala、java中，用来和强类型Datasets一起工作。而且用户不局限与这些预定义的聚合函数，可以创建他们自己的。 

#### 弱类型用户定义的聚合函数

用户需要继承UserDefinedAggregateFunction抽象类来实现个性化弱类型聚合函数。例如一个平均函数：

```java

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public static class MyAverage extends UserDefinedAggregateFunction {

  private StructType inputSchema;
  private StructType bufferSchema;

  public MyAverage() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
    inputSchema = DataTypes.createStructType(inputFields);

    List<StructField> bufferFields = new ArrayList<>();
    bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
    bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
    bufferSchema = DataTypes.createStructType(bufferFields);
  }
  // Data types of input arguments of this aggregate function
  public StructType inputSchema() {
    return inputSchema;
  }
  // Data types of values in the aggregation buffer
  public StructType bufferSchema() {
    return bufferSchema;
  }
  // The data type of the returned value
  public DataType dataType() {
    return DataTypes.DoubleType;
  }
  // Whether this function always returns the same output on the identical input
  public boolean deterministic() {
    return true;
  }
  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, 0L);
    buffer.update(1, 0L);
  }
  // Updates the given aggregation buffer `buffer` with new input data from `input`
  public void update(MutableAggregationBuffer buffer, Row input) {
    if (!input.isNullAt(0)) {
      long updatedSum = buffer.getLong(0) + input.getLong(0);
      long updatedCount = buffer.getLong(1) + 1;
      buffer.update(0, updatedSum);
      buffer.update(1, updatedCount);
    }
  }
  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
    long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
    buffer1.update(0, mergedSum);
    buffer1.update(1, mergedCount);
  }
  // Calculates the final result
  public Double evaluate(Row buffer) {
    return ((double) buffer.getLong(0)) / buffer.getLong(1);
  }
}

// Register the function to access it
spark.udf().register("myAverage", new MyAverage());

Dataset<Row> df = spark.read().json("examples/src/main/resources/employees.json");
df.createOrReplaceTempView("employees");
df.show();
// +-------+------+
// |   name|salary|
// +-------+------+
// |Michael|  3000|
// |   Andy|  4500|
// | Justin|  3500|
// |  Berta|  4000|
// +-------+------+

Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
result.show();
// +--------------+
// |average_salary|
// +--------------+
// |        3750.0|
// +--------------+
```

## 数据集

spark-sql支持在很多数据源上操作，通过DataFrame接口。DataFrame可以使用关系转关被操作，也能被用于创建临时视图。将DataFrame注册为一个暂时视图允许你在它的数据上运行sql查询。这一节描述用于使用spark数据源加载和保存数据的通用方法，并且查看一下具体的选项，这些选项对内置数据源是可用的。

### 通用加载、保存函数

最简单形式，默认数据源（parquet 除非通过spark.sql.sources.default进行配置了）会被用于所有的操作。

#### java实现

```java
Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

```

#### python实现

```python
df = spark.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
```

### 手动zhiding 选项

你可以手动指定数据源并且使用一些额外的参数，数据源由它们的全名来指定（org.apache.spark.sql.parquet)，但是对于内置数据源，你也可以用短名称（json,parquet,jdbc,orc,libsvm,csv,text)。DataFrames （从任何数据源类型加载而来的)可以被转换为其他类型。



#### python实现

```python
df = spark.read.load("examples/src/main/resources/people.json", format="json")
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
```

### 在文件上直接运行sql

我们不用读取数据的API来加载文件到DataFrame然后查询它，可以直接使用sql查询这个文件

#### python实现

```python

df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

```

### 保存模式

保存操作可以选择性的使用SaveMode,它指定了如何处理已有数据。意识到这些保存模式没有利用任何锁以及不是原子性的是很重要的。当要执行一个Overwrite（覆盖）,数据会在写出新数据之前被删除。

```JAVA

Scala/Java	Any Language	Meaning
SaveMode.ErrorIfExists (default)	"error" (default)	When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
SaveMode.Append	"append"	When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
SaveMode.Overwrite	"overwrite"	Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
SaveMode.Ignore	"ignore"	Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.

```

### 保存 到持久表格

DataFrame可以保存为持久表格到Hive metastore使用saveAsTable命令。注意到一个已有的Hive部署并不必要。spark会创建一个默认的本地hive metastore（使用Derby)。不想createOrReplaceTempView命令，saveAsTable会物质化DataFrame的内容，创建一个数据指针在hive metastore.持久表格会任然存在即使当你的spark程序已经重启之后，只要你维持你的连接到相同的metastore.一个用于持久表格的DataFrame可以通过调用saprksESSION的table方法带着表名称来创建。

对于基于文件的数据源，text,parquet,json等，你可以通过path选项来指定个性化的表路径，例如，df.write.option("path", "/some/path").saveAsTable("t")

表格被删除时，这个路径不会被删除，这个表格数据还在那儿。

如果没有指定路径，spark会写入数据到默认路径在warehouse目录。表格被删除时，这个默认的路径也会被删除。

从spark2.1开始，持久数据源表格具有存储在hive metastore的per-partition元数据。这会有以下好处

1 因为metastore可以对查询返回必要的分区，揭露所有的分区在第一个查询不再必须要。

2 Hvie DDL例如 alter table partition set location现在是可以对使用Datasource api创建的表格可用。



partition信息默认不收集，当创建外部数据源表格（使用一个路径选项的）。为了在metastore同步分区信息，可以调用MSCK REPAIR TABLE



### 分桶,排序和分区

对于基于文件的数据源，还能对输出进行分桶、排序和分区。分桶、排序支队持久表格可用。

```python
df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
```

当使用Dataset api，分区可以被save xxAsTable使用。



```python
df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
```

也可以对一个表格同时使用分区、分桶

```python
df = spark.read.parquet("examples/src/main/resources/users.parquet")
(df
    .write
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .saveAsTable("people_partitioned_bucketed"))
```

partitionBy创建了一个目录结构在ParitionDiscovery(分区发现)一节种描述。 因此，它有有限能力对于那些有高基数（high cardinality）的列。 bucketBy将数据发到固定数目的桶里面，当一些唯一值是无限多的时候可以被使用。

### parquet文件

它是一种列格式的，被很多数据处理系统支持的格式。spark-sql支持读写parquet文件，自动保存原始数的结构。当写parquet文件时，所有列自动转换为nullable类型出于兼容原因。

### 编程加载数据

使用以上例子种的数据：

#### python实现

```python 
peopleDF = spark.read.json("examples/src/main/resources/people.json")

# DataFrames can be saved as Parquet files, maintaining the schema information.
peopleDF.write.parquet("people.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = spark.read.parquet("people.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.show()
# +------+
# |  name|
# +------+
# |Justin|
# +------+
```





### 分区发现

表分区是一种常见优化方法在hive这样的系统种使用。在一个分区表格，数据通常存于不同目录，并且编码每个分区目录种的路径的列值进行分区。所有内置文件源（Text/CSV/JSON/ORC/Parquet）嫩巩固发现和推断分区信息。例如，我们把所有之前使用的生成的数据存到分区表格里面，使用下面的目录结构，使用额外2个列，性别和国家作为分区列。

```bash
path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...
```



通过传递path/to/table到 `SparkSession.read.parquet` or `SparkSession.read.load` 

spark-sql会自动从路径提取分区信息，返回的DataFrame的结构为：

```basic
root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)
```



注意分区列的数据类型时自动推断的。当前，数字数据类型，日期，时间戳以及字符串类型时支持的。又是，用户不想自动推断它们的类型。对于这种情况，自动推断可以通过spark.sql.sources.partitionColumnTypeInference.enabled配置实现，默认设置为true.当类型推断被禁止，字符串类型会被用于分区列上。

从1.6.0开始，分区防线只能找到在给定的目录下的分区。对于上面的例子，如果用户传递的是path/to/table/gender=male， 性别不会被考虑为一个分区列。 

如果需要指定基路径（分区路径应该开始的地方），可以在数据源选项设置basePath.例如，当path/to/table/gender=males是数据的路径，用户设置basePath为path/to/table/，性别就是分区列。



### 结构合并

parquet还支持结构进化，用户可从简单结构开始，逐渐加列到这个结构，这样，可能会有多个parquet文件带有不同的但是兼容的结构。parquet数据源现在能自动探测这种情况，合并所有这些文件的结构。

由于合并是一个贵操作，1.5.0版本开始默认关闭这个功能，通过以下步骤开启：

1 设置数据源选项mergeSchema 为true, 当读取parquet文件，或者

2 设置全局sql选项 `spark.sql.parquet.mergeSchema` 为true.

```python

from pyspark.sql import Row

# spark is from the previous example.
# Create a simple DataFrame, stored into a partition directory
sc = spark.sparkContext

squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, double=i ** 2)))
squaresDF.write.parquet("data/test_table/key=1")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11))
                                .map(lambda i: Row(single=i, triple=i ** 3)))
cubesDF.write.parquet("data/test_table/key=2")

# Read the partitioned table
mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
#  |-- double: long (nullable = true)
#  |-- single: long (nullable = true)
#  |-- triple: long (nullable = true)
#  |-- key: integer (nullable = true)
```

### hive metastore parquet 表格转换

从hive metastore parquet 表格读取以及写入的时候，sparksql会尝试使用自己的parquet支持而非hive serDe来获取更好的性能，这个行为被 `spark.sql.hive.convertMetastoreParquet`配置来控制，默认是开启的。

### hive /parquet结构协调

表结构处理的差异：

1 hive 大小写敏感

2 hive把所有列都看作nullable

当把hive metastore parquet表格转换为spark sql pquet表格是，会协调它们的结构：

1 结构中同名的与必须有相同数据类型。

2 协调后的结构只包含在hive 结构种定义的那些域。

​	任何指出吸纳字pquet结构种的域在协调后结构删除

​	任何会出现在hive 结构的域被添加作为nullable域到协调后结构里。

### JSON数据集

SPARK-SQL可以自动推荐JSON数据集的结构，加载为一个DataFrame,这通过使用SparkSession.read.json完成。

文件必须为一个json文件，但不是一个典型json文件，每行必须包含一个独立的有效的JSON对象。

请参考：http://jsonlines.org/

对于一个常规的多行json文件，设置multiLine为True

### python实现

```python
# spark is from the previous example.
sc = spark.sparkContext

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
path = "examples/src/main/resources/people.json"
peopleDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by spark
teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenagerNamesDF.show()
# +------+
# |  name|
# +------+
# |Justin|
# +------+

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string
jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
otherPeople.show()
# +---------------+----+
# |        address|name|
# +---------------+----+
# |[Columbus,Ohio]| Yin|
# +---------------+----+
```

### Hive表格

spark-sql支持读写存储在hive里的数据。到那时，由于hive有很多依赖，这些依赖不包含在spark里面，如果hive依赖可以在类路径找到，spark会加载它们。注意这些hive依赖必须要被所有work节点看到，因为它们需要获取到hive序列化、反序列化库serDes为了获得在hive里的数据。



通过把`hive-site.xml`, `core-site.xml` (for security configuration), and `hdfs-site.xml` (for HDFS configuration)文件放到conf/目录下完成hive配置。

当使用hive的时候，必须实例化sparksession，使用hive支持，包括连接到一个持久的hive metastore, hive serdes,以及hive的用户自定义函数。如果没有hive的部署，用户有何额能够获得hive支持。当没有配置hive-site.xml,context自动在当前目录创建metastore_db，通过spark.sql.warehouse.dir配置创建一个目录，它默认为spark-warehouse在当前目录，就是程序启动的地方。

注意，hive-site.xml 里的hive.metastore.warehouse.dir自从2.0.0开开始反对，使用spark.sql.warehouse.dir指定才仓库种数据库的默认位置。 可能需要给用户授权写的权限。

#### python实现

```python
from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# spark is an existing SparkSession
spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries are expressed in HiveQL
spark.sql("SELECT * FROM src").show()
# +---+-------+
# |key|  value|
# +---+-------+
# |238|val_238|
# | 86| val_86|
# |311|val_311|
# ...

# Aggregation queries are also supported.
spark.sql("SELECT COUNT(*) FROM src").show()
# +--------+
# |count(1)|
# +--------+
# |    500 |
# +--------+

# The results of SQL queries are themselves DataFrames and support all normal functions.
sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

# The items in DataFrames are of type Row, which allows you to access each column by ordinal.
stringsDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))
for record in stringsDS.collect():
    print(record)
# Key: 0, Value: val_0
# Key: 0, Value: val_0
# Key: 0, Value: val_0
# ...

# You can also use DataFrames to create temporary views within a SparkSession.
Record = Row("key", "value")
recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

# Queries can then join DataFrame data with data stored in Hive.
spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
# +---+------+---+------+
# |key| value|key| value|
# +---+------+---+------+
# |  2| val_2|  2| val_2|
# |  4| val_4|  4| val_4|
# |  5| val_5|  5| val_5|
# ...
```

### 分布式SQL引擎

#### 运行 Thrift jdbc odbc 服务器

在spark目录下运行：

```bash
./sbin/start-thriftserver.sh
```

服务器默认会在localhost:10000监听



#### 运行spark sql cli

在spark目录下运行：

```bash
./bin/spark-sql
```

