/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/

package org.zuinnote.spark.bitcoin.example


import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.BufferedOutputStream

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
*
* Demonstrate the HadoopCryptoLedger Spark Datasource API by loading the Bitcoin Blockchain.
* We modified the example of the author to write the schema of a block in a file.
*
*/

object SparkScalaBitcoinBlockDataSource {

	def main(args: Array[String]): Unit = {
        	val conf = new SparkConf().setAppName("BticoinBlockDataSource")
		val sc=new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		sumTransactionOutputsJob(sc,sqlContext,args(0),args(1))
		sc.stop()
	}

	def sumTransactionOutputsJob(sc: SparkContext, sqlContext: SQLContext, inputFile: String, outputFile: String): Unit = {
		val df = sqlContext.read.format("org.zuinnote.spark.bitcoin.block")
    							.option("magic", "F9BEB4D9") // set magic to the Bitcoin network
    							.load(inputFile)

		val hdfsConf = sc.hadoopConfiguration
		val fileSystem: FileSystem = FileSystem.get(hdfsConf)
		
		val hdfsFileOS: FSDataOutputStream = fileSystem.create(new Path("hdfs://" + outputFile));
		val bos = new BufferedOutputStream(hdfsFileOS)
		
		// Write schema file to HDFS 	
		bos.write(df.schema.treeString.getBytes("utf-8"))
		bos.close()
	}
}
