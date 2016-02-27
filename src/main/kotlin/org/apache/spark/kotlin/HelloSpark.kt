/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.kotlin

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import java.util.*

object HelloSpark {

    fun main(args: Array<String>) {
        val conf = SparkConf().setAppName("HelloSpark")
        val sc = JavaSparkContext(conf)
        body(sc)
        sc.stop()
    }

    fun body(sc: JavaSparkContext) {
        val sqlContext = SQLContext(sc)

        val data = Array(1000, { i -> i * i })
        val jData = ArrayList<Row>()
        for (i in data) {
            val row = RowFactory.create(i)
            jData.add(row)
        }
        val rdd = sc.parallelize(jData)

        val fields = ArrayList<StructField>()
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false))
        val schema = DataTypes.createStructType(fields)
        val df = sqlContext.createDataFrame(rdd, schema)
        print(df.count())
    }
}

