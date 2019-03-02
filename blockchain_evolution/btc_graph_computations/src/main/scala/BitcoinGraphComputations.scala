import it.unipd.dei.graphx.diameter.DiameterApproximation
import cn.edu.tsinghua.keg.spark.graphx.lib.LocalClusteringCoefficient

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.storage.StorageLevel

import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap


/**
 *
 * Loads the vertices and the edges of the graph and creates the Graph object. 
 * Then calculates metrics and prints their results to stdout.
 *
 * Metrics:
 *		- Size
 *		- Diameter
 *		- Triangle Participation Ratio (TPR)
 *		- Clustering Coefficient (Global and Average)
 *		- Bridge Ratio
 *		- Conductance
 *		
 */
object BitcoinGraphComputations {
	
	// Variables for bridge ratio
	var time = 0
	val visited = new HashMap[Int,Boolean]()
	val disc = new HashMap[Int,Int]()
	val low = new HashMap[Int,Int]()
	val parent = new HashMap[Int,Int]() { override def default(key:Int) = -1 }
	var bridges = 0

	/**
	 *
	 * @args Input Folder (Edges path at HDFS)
	 * 
	 */	
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Bitcoin Graph Computations")
		val sc = new SparkContext(conf)
		val hdfsConf = new Configuration();
		hdfsConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")	

		// Run computations for each year 
		for(year <- 2009 until 2018){

			//Load Graph per year
			val graph_year = loadGraph(sc, year, 0, args(0))
			graph_year.cache()

			// Computations per year
			computeSize(year, 0, "case_year", graph_year)
			computeDiameter(year, 0, "case_year", graph_year)
			computeTPR(year, 0, "case_year", graph_year)
			computeClusteringCoefficient(year, 0, "case_year", graph_year)

			val nbrss_year = graph_year.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
				var nbMap = ListBuffer.empty[VertexId]
				var i = 0
				while (i < nbrs.size) {
		  			
		  			// prevent self cycle
		  			val nbId = nbrs(i)
		  			if(nbId != vid) {
		    			nbMap += nbId
		  			}
		  			i += 1
				}
				nbMap
			}
			bridges = 0

			computeBridgeRatio(year, 0, "case_year", graph_year, nbrss_year)
			computeConductance(sc, year, 0, "case_year", graph_year)

			graph_year.unpersist()
		
			//Computations per month of each year
			var final_month = 13

			// Data for 2017 are until March
			if(year == 2017){
				final_month = 4
			}

			// Run computations for each month of the year
			for(month <- 1 until final_month){		
				
				//Load Graph per month
				val graph_month = loadGraph(sc, year, month, args(0))
				graph_month.cache()

				// Computations per month
				computeSize(year, month, "case_month", graph_month)
				computeDiameter(year, month, "case_month", graph_month)
				computeTPR(year, month, "case_month", graph_month)
				computeClusteringCoefficient(year, month, "case_month", graph_month)

				val nbrss_month = graph_month.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
					var nbMap = ListBuffer.empty[VertexId]
					var i = 0
					while (i < nbrs.size) {
			  			
			  			// prevent self cycle
			  			val nbId = nbrs(i)
			  			if(nbId != vid) {
			    			nbMap += nbId
			  			}
			  			i += 1
					}
					nbMap
				}
				bridges = 0
				
				computeBridgeRatio(year, month, "case_month", graph_month, nbrss_month)
				computeConductance(sc, year, month, "case_month", graph_month)

				graph_month.unpersist()
			} 
		}

		sc.stop()
	}



	// Loads the graph per year or per month
 	def loadGraph(sc: SparkContext, year: Int, month: Int, inputFile:String): Graph[Int, Int] = {
 		val sqlContext = new SQLContext(sc)
 		import sqlContext.implicits._
 		
 		var edges_parq = sqlContext.emptyDataFrame

 		if(month == 0){
 			edges_parq = sqlContext.read.option("header","true").parquet(inputFile + "/year=" + year)
 		}else{
 			edges_parq = sqlContext.read.option("header","true").parquet(inputFile + "/year=" + year + "/month=" + month) 
 		}
		
	    val edges: RDD[Edge[Int]] = edges_parq.rdd.map(row => Edge(row(0).asInstanceOf[Int].toInt, row(1).asInstanceOf[Int].toInt))
	    val graph = Graph.fromEdges(edges, 1.toInt)
		graph
	}



	// Computes the size of the given graph
	def computeSize(year:Int, month: Int, computeCase: String, graph: Graph[Int, Int]): Unit = {
		if(computeCase == "case_year"){
			println(year + "," + graph.numEdges)
		}else{
			println(year + "," + month + "," + graph.numEdges)
		}
	}



	// Computers the diameter of the given graph
	def computeDiameter(year:Int, month: Int, computeCase: String, graph: Graph[Int, Int]): Unit = {
		val g = graph.mapEdges{e => 1.0}.groupEdges(math.min)
    	val ungraph = g.mapEdges { e => Random.nextDouble() + 1 }

   		// Calculate the diameter 
    	val d = DiameterApproximation.run(ungraph)

		if(computeCase == "case_year"){
			println(year + "," + d.toString)
		}else{
			println(year + "," + month + "," + d.toString)
		}
	}



	// Computes the triangle participation ratio on the given graph
	def computeTPR(year:Int, month: Int, computeCase: String, graph: Graph[Int, Int]): Unit = {
		
		val triangles: Long = graph.triangleCount().vertices.filter { case (vid, data) => data != 0 }.count()

		val num_vertices = graph.numVertices
		val tpr = (1.0*triangles) / num_vertices
		
		if(computeCase == "case_year"){
			println(year + "," + tpr)
		}else{
			println(year + "," + month + "," + tpr)
		}
	}


	
	// Computes the global and the average clustering coefficient on the given graph
	def computeClusteringCoefficient(year:Int, month: Int, computeCase: String, graph: Graph[Int, Int]): Unit = {

		// Compute Global CC
    	var numberOfTriplets = graph.triplets.count

    	// Count triangles
    	var numberOfTriangles: Int = 0
    	val triangleCounter = graph.triangleCount()
    	val verts = triangleCounter.vertices
    	verts.collect().foreach { case (vid, count) =>
      		numberOfTriangles += count
    	}
    	
    	numberOfTriangles /= 3 
    
    	var GLOBAL_CC = 0f
    	if (numberOfTriplets > 0){
      		val floatTriangles = numberOfTriangles.toFloat
      		GLOBAL_CC = 3 * floatTriangles / numberOfTriplets
    	}


    	// Compute Average CC
		// Take the mean of the count below and it will be average CC! 
	    // Without the mean it is the local for each node
	    val lcc = LocalClusteringCoefficient.run(graph)
		
	    val verts_avg = lcc.vertices
	    
	    val LOCAL_CC = verts_avg.filter { case (vID, count) => count > 0 }

	    var sum_count = verts_avg.map(_._2).sum()
	    val AVG_CC = sum_count / graph.numVertices

	    if(computeCase == "case_year"){
			println(year + "," + GLOBAL_CC + "," + AVG_CC)
		}else{
			println(year + "," +  month + "," + GLOBAL_CC + "," + AVG_CC)
		}
	}
	


	// Computes the bridge ratio on the given graph
	def computeBridgeRatio(year:Int, month: Int, computeCase: String, graph: Graph[Int, Int], nbrss: RDD[(VertexId, ListBuffer[VertexId])]) = {

		var v = graph.numVertices.toInt
		var e = graph.numEdges.toInt
		
		val x = graph.vertices.map(id => {
			val vertex = id._1 
			val u = vertex.toInt
			u	
		}).collect.foreach(u => {visited += (u -> false)})

		for ((k,v) <- visited){
			if(visited(k) == false){
				bridgeUtil(nbrss, k, visited, disc, low, parent) 	
			}
		}
		
		val d_bridges = bridges.toDouble
		val d_edges = e.toDouble
		val bridge_ratio = d_bridges/d_edges

		if(computeCase == "case_year"){
			println(year + "," + bridge_ratio)
		}else{
			println(year + "," +  month + "," + bridge_ratio)
		}
	}


	
	// DFS bridge finding algorithm 
	def bridgeUtil(nbrss: RDD[(VertexId, ListBuffer[VertexId])], U: Int, visited: HashMap[Int,Boolean], disc: HashMap[Int,Int], low: HashMap[Int,Int], parent: HashMap[Int,Int]):Unit= {
		time += 1
		val u = U
		visited(u) = true
		disc(u) = time
		low(u) = time
		
		// Look up for the neighbors of specific vertex
		var v_nbrs = nbrss.filter{case a => a._1 == u}.map(b => b._2).collect
		
		for(k <- 0 until v_nbrs(0).length){
		
			var v_l = v_nbrs(0)(k);
			var v = v_l.toInt
		
			if(!visited(v)){
				parent(v) = u
				bridgeUtil(nbrss, v, visited, disc, low, parent)
				low(u) = math.min(low(u), low(v))
				if(low(v) > disc(u)){
					bridges+=1
				}
			}else if(v != parent(u)){
				low(u) = math.min(low(u),disc(v))
			}
		}
	}



	// Computes the conductance of the given graph. In case of year, it stores the vertices 
	// and the edges of the community that  that was used to compute the conductance
	def computeConductance(sc: SparkContext, year:Int, month: Int, computeCase: String, graph: Graph[Int, Int]): Unit = {

		//	Get the community ID for each vertex
		val communityGraph = org.apache.spark.graphx.lib.LabelPropagation.run(graph, 10)


		//	Filter out communities with less than 10 vertices (comID, sizeOfCommunity)
		val totalCommunities = communityGraph.vertices.map{case (v,d) => d}.groupBy(x => x).map{case (a,b) => (a,b.size) }
		val filteredCommunities = totalCommunities.filter{case (a,b) => b >= 10 }
 
		// Select only the top communitiy (comID, sizeOfCommunity)
		val communitiesSelected = filteredCommunities.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
		
		// Collect the IDs of the selected communities (comIDs)
		val communityIds = communitiesSelected.map{case (id,d) => id}.toList
		sc.parallelize(communityIds)

		// Construct the graph of the selected communties
		// Store the edges and their vertices for graph visualization
		val allCommunitiesGraph = communityGraph.subgraph(epred = { e => communityIds.contains(e.srcAttr) || communityIds.contains(e.dstAttr) })
		val totalCommunitiesEdges = allCommunitiesGraph.edges.count
		

		// Compute conductance
		var conductances = scala.collection.mutable.Map[Long, Double]()
		var counter = 0
		communityIds.foreach{ cx_id => {
		
				val cutEdgesRDD = allCommunitiesGraph.subgraph(epred = { e => e.srcAttr == cx_id})
				val outEdgesRDD = cutEdgesRDD.subgraph(epred = { e => e.dstAttr != cx_id})
			
				var cutEdges = cutEdgesRDD.numEdges
				var outEdges = outEdgesRDD.numEdges
				
				if(cutEdges > 0.0 && outEdges > 0.0){	
					conductances += (cx_id -> (outEdges.toDouble / cutEdges.toDouble))
				}else{
					conductances += (cx_id -> 0.0)
				}

				counter = counter + 1
			}
		}

		
		var validConductances = conductances.filter{case (k,v) => v > 0}

		// Store the community from which the conductance of the graph is computed
		val tmp = validConductances.minBy(_._2)
		val minID = tmp._1
		val minConductance = tmp._2

		if(computeCase == "case_year"){
			var community = allCommunitiesGraph.subgraph(epred = { e => e.srcAttr == minID || e.dstAttr == minID})

			// Vertices	
			community.vertices.filter{ case (vid, comID) => comID == minID}.map{ case (v,c) => v + "," + c}.saveAsTextFile("/user/lsde08/graph_communities/vertices_" + year)

			//Edge(src,dst,weight)
			community.edges.map{ e => e.srcId + "," + e.dstId}.saveAsTextFile("/user/lsde08/graph_communities/edges_" + year)
		}
 
		
		// Print result and some statistics
		if(computeCase == "case_year"){
			println("Conductance for " + year + ": " + minConductance)
			println("Total communities for " + year + ": " + totalCommunities.count + " After filtering >10: " + filteredCommunities.count)
			println("Graph total edges for " + year + ": " + graph.numEdges)
			println("Selected communities edges for " + year + ": " + totalCommunitiesEdges+"\n")
		}else{
			println("Conductance for " + month + "/" + year + ": " + minConductance)
			println("Total communities for " + month + "/" + year + ": " + totalCommunities.count + " After filtering >10: " + filteredCommunities.count)
			println("Graph total edges for " + month + "/" + year + ": " + graph.numEdges)
			println("Selected communities edges for " + month + "/" + year + ": " + totalCommunitiesEdges + "\n")
		} 
	}
}
