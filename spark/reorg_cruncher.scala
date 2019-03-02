import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SQLContext, SaveMode}
import scala.collection.mutable.ArrayBuffer

def reorg(datadir :String) 
{
	val t0 = System.nanoTime()
	
	// Read CSV files
	val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/person.*csv.*")
	val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/interest.*csv.*")
	val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/knows.*csv.*")  

	// Remove unused columns from person table
	val new_person = person.drop("firstName").drop("lastName").drop("gender").drop("creationDate").drop("locationIP").drop("browserUsed")


	// Keep only person knows from the same location
	val pairs = new_person.select($"personId", $"locatedIn".alias("ploc"), $"birthday").
                      		  join(knows, "personId")
	
	val person_knows = new_person.select($"personId".alias("friendId"), $"locatedIn".alias("floc")).
                       		      join(pairs, "friendId").
                       		      filter($"ploc"===$"floc")


	// Check for mutual friendship
	val p_k = person_knows.select($"personId".alias("p"), $"friendId".alias("f"))
	
	val bidir    = p_k.join(person_knows.select($"personId".alias("f"), $"friendId", $"birthday"), "f").
						filter($"p"===$"friendId").
						withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday"))

						//.withColumn("personId", $"f").
						//withColumn("friendId", $"friendId").

	val new_knows = bidir.withColumnRenamed("f", "personId").drop("p").drop("birthday")

	// Remove people with no friends (according to unique personIds in knows table) and
	// sort them by birthday ascending	
	val new_person_has_friends = (new_knows.select($"personId").distinct()).
				     join(new_person, "personId").
				     orderBy($"birthday".asc)	

	// Write to parquet files
	new_person_has_friends.write.mode(SaveMode.Overwrite).parquet(datadir+"/person_par")
	interest.write.mode(SaveMode.Overwrite).parquet(datadir+"/interest_par")  
	new_knows.repartition($"bday").write.mode(SaveMode.Overwrite).partitionBy("bday").parquet(datadir+"/knows_par")
	
	val t1 = System.nanoTime()
	println("reorg time: " + (t1 - t0)/1000000 + "ms") 
}

def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int) :org.apache.spark.sql.DataFrame =
{
	val t0 = System.nanoTime()

	// List of files to read
	val fileList = ArrayBuffer[String]()
	
	// Check for existing file
	val conf = sc.hadoopConfiguration
	val fs = org.apache.hadoop.fs.FileSystem.get(conf);
	
	for(x <- lo to hi){
		val c = datadir + "/knows_par/bday=" + x
		
		val exists = fs.exists(new org.apache.hadoop.fs.Path(c))
		
		if(exists){
			fileList += c	
		}
	} 

	// load the three tables	
	val person   = spark.read.option("header","true").parquet(datadir+"/person_par")
	val interest = spark.read.option("header","true").parquet(datadir+"/interest_par")
	val knows    = spark.read.option("header","true").parquet(fileList: _*)


	// select the relevant (personId, interest) tuples, and add a boolean column "nofan" (true iff this is not a a1 tuple)
	val focus    = interest.filter($"interest" isin (a1, a2, a3, a4)).
                          withColumn("nofan", $"interest".notEqual(a1))

	// compute person score (#relevant interests): join with focus, groupby & aggregate. Note: nofan=true iff person does not like a1
	val scores   = person.join(focus, "personId").
                        groupBy("personId", "locatedIn", "birthday").
                        agg(count("personId") as "score", min("nofan") as "nofan")

	// filter (personId, score, locatedIn) tuples with score>1, being nofan, and having the right birthdate
	val cands    = scores.filter($"score" > 0 && $"nofan")
                       //.withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")).
                       //filter($"bday" >= lo && $"bday" <= hi)

	// create (personId, ploc, friendId, score) pairs by joining with knows (and renaming locatedIn into ploc)
	val pairs    = cands.select($"personId", $"locatedIn".alias("ploc"), $"score").
                       join(knows, "personId")

	// re-use the scores dataframe to create a (friendId, floc) dataframe of persons who are a fan (not nofan)
	val fanlocs  = scores.filter(!$"nofan").select($"personId".alias("friendId"), $"locatedIn".alias("floc"))

	// join the pairs to get a (personId, ploc, friendId, floc, score), and then filter on same location, and remove ploc and floc columns
	val results  = pairs.join(fanlocs, "friendId").
                       select($"personId".alias("p"), $"friendId".alias("f"), $"score")

	// keep only the (p, f, score) columns and sort the result
	val ret      = results.select($"p", $"f", $"score").orderBy(desc("score"), asc("p"), asc("f"))

	ret.show(1000) // force execution now, and display results to stdout

	val t1 = System.nanoTime()
	println("cruncher time: " + (t1 - t0)/1000000 + "ms")

	return ret  
}
