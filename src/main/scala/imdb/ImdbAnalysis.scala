package imdb

import math._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf().setAppName("IMDB Analysis").setMaster("local") ;
  val sc: SparkContext = new SparkContext(conf);


  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics(_))

  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings(_))

  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew(_))

  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics(_))



  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
  	val filteredRDD = rdd
  	.filter(x => x.runtimeMinutes.getOrElse(-1) >= 0 && x.genres.getOrElse(List())!= List() )
  
  	
  	val genreRuntime = filteredRDD
  	.flatMap(x => x.genres.getOrElse(List()).map(y=> (y,x.runtimeMinutes.getOrElse(-1))))
  	
  	type GenreCollector = (Int,Int,Int,Int)
  	type GenreData = (String,(Int,Int,Int,Int))


  	val createGenreCombiner = (runtime:Int) => (runtime,1,runtime,runtime)

  	
  	val genreCombiner = (collector: GenreCollector, runtime: Int) => {
  		val (cumTime,numMovies,minTime,maxTime) = collector
  		(cumTime+runtime,numMovies+1,math.min(runtime,minTime),math.max(runtime,maxTime))
  	}

  	val genreMerger = (c1: GenreCollector, c2: GenreCollector) => {
  		val (cumTime1,numMovies1,minTime1,maxTime1) = c1
  		val (cumTime2,numMovies2,minTime2,maxTime2) = c2

  		(cumTime1+cumTime2,numMovies1+numMovies2,math.min(minTime1,minTime2),math.max(maxTime1,maxTime2))
  	}


  	val results: RDD[(GenreData)] = genreRuntime.combineByKey(createGenreCombiner,genreCombiner,genreMerger)


  	val formatResults = (aGenre: GenreData) => {
  		val (genre, (cumTime,numMovies,minTime,maxTime)) = aGenre

  		(cumTime.toFloat/numMovies,minTime,maxTime,genre)
  	}

  	val formattedResults = results.map(formatResults)

    return formattedResults
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {


 	val filteredRating = l2.filter(x => (x.numVotes >= 500000 && x.averageRating >= 7.5)).map( x => (x.tconst,true))
 	val filteredTitles = l1.filter( x => (x.runtimeMinutes.getOrElse(-1)>=0 && (1990 <= x.startYear.getOrElse(0) && x.startYear.getOrElse(0) <= 2018) && x.titleType.getOrElse("") == "movie"))
 	 .map(x => (x.tconst,x.primaryTitle.getOrElse("")))
	val joined = filteredTitles.join(filteredRating).map(x => x._2._1)

	return joined

  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    
    val ratingsfiltered = l2.filter(x => x.averageRating >= 0)
  	val ratingsMap = ratingsfiltered.map(x => (x.tconst,x.averageRating))
  	val ratingsLookup = sc.broadcast(ratingsMap.collectAsMap())

  	val filtered  = l1.filter(x => ratingsLookup.value.getOrElse(x.tconst,-1.0f) >= 0 && x.titleType.getOrElse("") == "movie" 
  		&& x.startYear.getOrElse(0) >= 1900 && x.startYear.getOrElse(0) <= 1999)

  	val decadeGenre_TitleRating =  filtered.flatMap(x => x.genres.getOrElse(List()).map(y=> 
  		(((x.startYear.getOrElse(-1)/10)%10,y),  (ratingsLookup.value.getOrElse(x.tconst,-1.0f),x.primaryTitle.getOrElse("")))
  	
  	))
  	type GenreCollector = (Float,String)
  	
  	type GenreData = ((Int,String),(Float,String))

	val createGenreCombiner = (data:GenreCollector) => (data._1, data._2)

  	val genreCombiner = (collector: GenreCollector, data: GenreCollector) => {
  		val (curRating,curTitle) = collector

  		val ans = if(data._1  == curRating){ 
					if(data._2 < curTitle){
  						data
  						}
  					else{(collector)
  					}
  				}
  					else if(data._1 > curRating){
  						data
  					} 
  		
  		else{
  			collector
  		
  		}

  		ans
  	}

  	val genreMerger = (c1: GenreCollector, c2: GenreCollector) => {
  		val (curRating1,curTitle1) = c1
  		val (curRating2,curTitle2) = c2


  		val ans = if(curRating1  == curRating2){ 
					if(curTitle1 < curTitle2){
  						c1
  						}
  					
  					else {
  						c2
  					}
  				}
  					else if(curRating1 > curRating2){
  					c1 
  		} 
  		
  		else{
  			c2
  		
  		}

  		ans

  	}


	val results: RDD[(GenreData)] = decadeGenre_TitleRating.combineByKey(createGenreCombiner,genreCombiner,genreMerger)


	val formatResults = (genreResults: GenreData) => {
  		val ((decade,genre),(score,title)) = genreResults

  		(decade,genre,title)
  	}

  	val formattedResults = results.map(formatResults)

  	val sortedResults = formattedResults.sortBy(e => (e._1, e._2))

  	return sortedResults
  


  }

 
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {

  	
  	val validMovies = l1
  	.map(x => { if((2010 <= x.startYear.getOrElse(0) && x.startYear.getOrElse(0) <= 2021)) (x.tconst,1) 
  		else (x.tconst,0)
     }) 
  
  	val movies = sc.broadcast(validMovies.collectAsMap())
	

	def param0= (accu:Int, v:String) => accu + movies.value.getOrElse(v,0)
  def param1= (accu1:Int, accu2:Int) => accu1 + accu2
	val actors = l3.map(x => (x.primaryName.getOrElse(""),x.knownForTitles.getOrElse(List()).aggregate(0)(param0,param1)))
	
	val validactors = actors.filter(x => x._2 >1)
    return validactors 
  }






  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
