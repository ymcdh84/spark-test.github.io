package com.iljin.scala

import java.util.Calendar
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.util.LongAccumulator
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.bson._

import scala.util.Try

class MongoDBForeachWriter (p_uri: String,
                            p_dbName: String,
                            p_collectionName: String,
                            p_messageCountAccum: LongAccumulator) extends ForeachWriter[Row]{

  val mongodbURI = p_uri
  val dbName = p_dbName
  val collectionName = p_collectionName
  val messageCountAccum = p_messageCountAccum

  var mongoClient: MongoClient = null
  var db: MongoDatabase = null
  var collection: MongoCollection[Document] = null

  def ensureMongoDBConnection(): Unit = {
    if (mongoClient == null) {
      mongoClient = MongoClient(mongodbURI)
      db = mongoClient.getDatabase(dbName)
      collection = db.getCollection(collectionName)
    }
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(record: Row): Unit = {

    val valueStr = record.getAs[String]("value")

    val doc: Document = Document(valueStr)
    doc += ("log_time" -> Calendar.getInstance().getTime())

    // lazy opening of MongoDB connection
    ensureMongoDBConnection()

    println("Document JSON = " + doc)

    val result = collection.insertOne(doc)

    println("Insert One Result = " + result)

    // tracks how many records I have processed
    if (messageCountAccum != null) {
      messageCountAccum.add(1)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {

    if(mongoClient != null) {
      Try {
        mongoClient.close()
      }
    }
  }
}
