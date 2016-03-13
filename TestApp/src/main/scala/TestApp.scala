/* TestApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable
import org.apache.spark.mllib.clustering.{EMLDAOptimizer, OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import org.apache.spark.storage._

object TestApp {
  def main(args: Array[String]) {
  
    
    val conf = new SparkConf().setAppName(args(0)+args(1)+args(2))
    val sc = new SparkContext(conf)
   
  
	//val corpusPath = "/home/ccortes/NYTimes/docword.nytimes.txt" // Should be some file on your system

	val corpusPath = "hdfs://reed:54310/user/ccortes/"+args(0)
	// Load documents from text files, 1 row per word
	val rawCorpus = sc.textFile(corpusPath )
	
	//Quitamos las tres primeras filas
	val firstRow = rawCorpus.first
	val tempCorpus1: RDD[String] = rawCorpus.filter(x=> x!=firstRow)
	
	val secondRow = tempCorpus1.first
	val tempCorpus2: RDD[String] = tempCorpus1.filter(x=> x!=secondRow)
	
	val thirdRow = tempCorpus2.first
	val tempCorpus3: RDD[String] = tempCorpus2.filter(x=> x!=thirdRow)
		
	//Separamos los strings por los espacios nos queda cada row de la forma Array[String](DocID, WordID, WordCount)
	val StringCorpus = tempCorpus3.map(x=> x.split(' '))

	//Pasamos el array de String a array de double
	val doubleCorpus = StringCorpus.map(x=>(x(0).toLong, Array( x(1).toDouble, x(2).toDouble)))

	
	/* 
	Ahora tenemos un RDD de la siguiente forma:
	
	Row 1: (Double, Array[Double]) = ( DocID, Array( WordID, WordCount))
	
	Lo que queremos conseguir es un RDD que tenga la siguiente forma:
	
	Row 1: (Long, Vector)  =  (DocID, [countWord1, countWord2, ... ])
	
	
	*/
	
	//Ahora mismo tenemos: Row 1: (Double, Array[Double]) = ( DocID, Array( WordID, WordCount, WordID, WordCount, ...)) es decir ya hemos agrupado todas las palabras de cada documento
	val reducedCorpus = doubleCorpus.reduceByKey((x,y)=>x++y)
	
	//Ahora vamos de dos en dos por el hechod e tener WordID, WordCount i ponimos en el hashMap key=WordID, value=WordCount. Hacemos un hashMap porque es rapido y luego es muy facil pasar a Vector.sparse que es lo que usa LDA


	val sparseCorpus = reducedCorpus.map{ case (docID, wordCount) =>
		val counts = new mutable.HashMap[Int, Double]()
		var i=0
		var j=0
		for (i <- 0 to (wordCount.size-1) by 2 ){
		
			counts(wordCount(i).toInt)= wordCount(i+1);
		}
		
		(docID, Vectors.sparse(102661, counts.toSeq)) //el 102661 es el tamaño del vocab. por eso no cambia con cada tamaño del dataset. Lo que partimos es el tamaño y tal y como viene estamos quitando documentos. 
	}
	
	val numTopics = 10
	val lda = new LDA().setK(numTopics).setMaxIterations(50)
	lda.setOptimizer(args(1))
	
		
	sparseCorpus.persist(StorageLevel.MEMORY_AND_DISK)
	val ldaModel = lda.run(sparseCorpus)
	
	
	//No queremos ver los topics en las pruebas de rendimiento
	//val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
	

	//topicIndices.foreach(println)

	
	
	}
}


/*
Funcionamiento del HashMap

val counts = new mutable.HashMap[Int, Double]()
counts +=(1 -> 2.0)


*/


// (id, Vectors.sparse(vocab.size, counts.toSeq))



























