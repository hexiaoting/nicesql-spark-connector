package com.nicesql.spark.connector
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


/**
  * Created by hewenting on 17/9/5.
  */
class NiceSQLPartition(idx:Int) extends Partition {
  override def index: Int = idx
}

class NiceSQLRDD(sc:SparkContext,location:String) extends RDD[Row](sc, Nil){
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new NextIterator[Row] {
    val index:Int = split.asInstanceOf[NiceSQLPartition].index
    var count:Int = 0

    override def getNext(): Row = {
      count = count + 1
      println("index="+index+",count="+count+",finished="+finished)
      if (count == 3)
        finished = true
      Row(index,count)
    }

    override def close(): Unit = {
      null
    }
  }

//  override def getDependencies: Seq[Dependency[_]] = {
//    sys.error("getDependency")
//    null
//  }

  override def getPartitions: Array[Partition] = {
    val result = new Array[Partition](4)
    for(i<- 0 until(result.length)) {
      result.update(i, new NiceSQLPartition(i))
    }
    result
  }
}