package org.zuinnote.spark.bitcoin

import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.BufferedOutputStream
import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._
import org.apache.spark.graphx._

import java.io._

/**
*
* Reads blocks and constructs a dataframe of transactions. Then creates the edges of the graph and partitions them
* to the files according to year and month. We use the example (scala-spark-graphx-bitcointransactions) of HadoopCryptoledger 
* to get generate the Block and Transactions objects as well as the operations that does in order to create the graph with 
* with some modifications.
*
*/

object BitcoinTransactionsToGraphPartitioning {
	
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("BitcoinTransactionsToFiles")
		val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val hadoopConf = new Configuration();
		hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic","F9BEB4D9");
		transactionsToGraphPartitions(sc,sqlContext,hadoopConf,args(0),args(1))
		sc.stop()
	}


	def transactionsToGraphPartitions(sc: SparkContext, sqlContext: SQLContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
		import sqlContext.implicits._

		val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock],hadoopConf)

		// Extract a tuple per transaction containing Bitcoin destination address, the input transaction hash, the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
	   	val btcTuples = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))

		val rowRDD = btcTuples.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6))

		val transactionSchema = StructType(
	        	Array(  
	        		StructField("dest_address", StringType, true),
	                	StructField("curr_trans_input_hash", BinaryType, false),
	                	StructField("curr_trans_input_output_idx", LongType, false),
	                	StructField("curr_trans_hash", BinaryType, false),
	                	StructField("curr_trans_output_idx", LongType, false),
	                	StructField("timestamp", IntegerType, false) 
	            	)
	    	)

		val btcDF = sqlContext.createDataFrame(rowRDD, transactionSchema)     

		val btcDF_Date_Timestamp =  btcDF.select($"dest_address", $"curr_trans_input_hash", $"curr_trans_input_output_idx", $"curr_trans_hash", $"curr_trans_output_idx", $"timestamp").
						  			  withColumn("realTime", to_date(from_unixtime($"timestamp"))).
                          			  withColumn("year", year($"realTime")).
                         			  withColumn("month", month($"realTime"))

		val btcDF_Date = btcDF_Date_Timestamp.drop("timestamp").drop("realTime")

        // Extract a tuple per transaction containing Bitcoin destination address, the input transaction hash, the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
		val bitcoinTransactionTuples = btcDF_Date.rdd.map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Array[Byte]], row(2).asInstanceOf[Long], row(3).asInstanceOf[Array[Byte]], row(4).asInstanceOf[Long], row(5).asInstanceOf[Int], row(6).asInstanceOf[Int]))

		// (bitcoinAddress, vertexId) 
		val bitcoinAddressIndexed = bitcoinTransactionTuples.map(bitcoinTransactions =>(bitcoinTransactions._1)).distinct().zipWithIndex()

		// Create edges
		// This is basically a self join, where ((currentTransactionHash,currentOutputIndex), identfier) is joined with ((inputTransactionHash,currentinputIndex), indentifier)

		// (bitcoinAddress,(byteArrayTransaction, TransactionIndex))
		val inputTransactionTuple =  bitcoinTransactionTuples.map(bitcoinTransactions => (bitcoinTransactions._1,(new ByteArray(bitcoinTransactions._2),bitcoinTransactions._3)))

		// (bitcoinAddress, ((byteArrayTransaction, TransactionIndex), vertexId))
		val inputTransactionTupleWithIndex = inputTransactionTuple.join(bitcoinAddressIndexed)

		// ((byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress))
		val inputTransactionTupleByHashIdx = inputTransactionTupleWithIndex.map(iTTuple => ((iTTuple._2._1._1, iTTuple._2._1._2), (iTTuple._2._2, iTTuple._1)))


		// (bitcoinAddress,(byteArrayTransaction, TransactionIndex, Year, Month))
		val currentTransactionTuple =  bitcoinTransactionTuples.map(bitcoinTransactions => (bitcoinTransactions._1,(new ByteArray(bitcoinTransactions._4),bitcoinTransactions._5,bitcoinTransactions._6,bitcoinTransactions._7)))

		// (bitcoinAddress, ((byteArrayTransaction, TransactionIndex, Year, Month), vertexId))
		val currentTransactionTupleWithIndex = currentTransactionTuple.join(bitcoinAddressIndexed)
		
		// ((byteArrayTransaction, TransactionIndex), (vertexId, (Year, Month, bitcoinAddress))
		val currentTransactionTupleByHashIdx = currentTransactionTupleWithIndex.map(iTTuple => ((iTTuple._2._1._1, iTTuple._2._1._2), (iTTuple._2._2, (iTTuple._2._1._3, iTTuple._2._1._4, iTTuple._1))))

		// The join creates ((ByteArray, Idx), ((srcIdx, srcAdress), (destIdx, (Year, Month, destAddress))) 
		val joinedTransactions = inputTransactionTupleByHashIdx.join(currentTransactionTupleByHashIdx)	

		// -> Edge([srcIdx, destIdx])
		// Convert joinedTransactions to DF keeping only those values that we need
		val edgeRDD = joinedTransactions.map(p => Row(p._2._1._1.toInt, p._2._2._1.toInt, p._2._2._2._1, p._2._2._2._2))
		
		val edgeSchema = StructType(
	        	Array(  
	                	StructField("src", IntegerType, true),
	                	StructField("dest", IntegerType, false),
	                	StructField("year", IntegerType, false),
	                	StructField("month", IntegerType, false)
	            	)
	    	)

		val edgeDF = sqlContext.createDataFrame(edgeRDD, edgeSchema)
		edgeDF.write.mode(SaveMode.Overwrite).partitionBy("year", "month").parquet(outputFile) 
	}



	/**
	*
	* This function was taken from HadoopCryptoledger and generates Transactions objects from Transactions List in the block. We just added the timestamp as a part
	* of the transation.
	*
	*/
	def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[(String,Array[Byte],Long,Array[Byte], Long, Int)] = {
		// First we need to determine the size of the result set by calculating the total number of inputs multiplied by the outputs of each transaction in the block
		val transactionCount = bitcoinBlock.getTransactions().size()
		var resultSize = 0

		for (i<-0 to transactionCount-1) {
			resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size()*bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
		}
		
		// Then we can create a tuple for each transaction input: Destination Address (which can be found in the output!), Input Transaction Hash, Current Transaction Hash, Current Transaction Output
		// as you can see there is no 1:1 or 1:n mapping from input to output in the Bitcoin blockchain, but n:m (all inputs are assigned to all outputs), cf. https://en.bitcoin.it/wiki/From_address
		val result:Array[(String,Array[Byte],Long,Array[Byte], Long, Int)] = new Array[(String,Array[Byte],Long,Array[Byte],Long, Int)](resultSize)
		var resultCounter: Int = 0

		for (i <- 0 to transactionCount-1) { // For each transaction
			val currentTransaction = bitcoinBlock.getTransactions().get(i)
			val currentTransactionHash = BitcoinUtil.getTransactionHash(currentTransaction)

			for (j <-0 to  currentTransaction.getListOfInputs().size()-1) { // For each input
				val currentTransactionInput = currentTransaction.getListOfInputs().get(j)
				val currentTransactionInputHash = currentTransactionInput.getPrevTransactionHash()
				val currentTransactionInputOutputIndex = currentTransactionInput.getPreviousTxOutIndex()
				
				for (k <-0 to currentTransaction.getListOfOutputs().size()-1) {
					val currentTransactionOutput = currentTransaction.getListOfOutputs().get(k)
					var currentTransactionOutputIndex = k.toLong
					result(resultCounter) = (BitcoinScriptPatternParser.getPaymentDestination(currentTransactionOutput.getTxOutScript()),currentTransactionInputHash,currentTransactionInputOutputIndex,currentTransactionHash,currentTransactionOutputIndex, bitcoinBlock.getTime())
					resultCounter+=1
				}
			}
		}
		result;
	}


	def serialise(value: Any): Array[Byte] = {
		val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
		val oos = new ObjectOutputStream(stream)
		oos.writeObject(value)
		oos.close()
		stream.toByteArray
	}
}


/**
 *
 * Helper class to make byte arrays comparable
 *
 */
class ByteArray(val bArray: Array[Byte]) extends Serializable {
	override val hashCode = bArray.deep.hashCode
	override def equals(obj:Any) = obj.isInstanceOf[ByteArray] && obj.asInstanceOf[ByteArray].bArray.deep == this.bArray.deep
}
