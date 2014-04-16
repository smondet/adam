/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.apache.avro.generic.IndexedRecord
import scala.collection.JavaConversions._
import org.bdgenomics.adam.util.ParquetFileTraversable
// import org.apache.spark.api.java
import java.util
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.avro.data.Json
import org.apache.avro.io.DecoderFactory
import org.bdgenomics.adam.avro.{ ADAMVariant, ADAMRecord }
import org.apache.avro.specific.SpecificRecord
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object PrintADAM extends ADAMCommandCompanion {
  val commandName: String = "print"
  val commandDescription: String = "Print an ADAM formatted file"

  def apply(cmdLine: Array[String]) = {
    new PrintADAM(Args4j[PrintADAMArgs](cmdLine))
  }
}

class PrintADAMArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
  var filesToPrint = new util.ArrayList[String]()

  @Args4jOption(required = false, name = "-raw", usage = "Display raw, non-formatted JSON")
  var displayRaw: Boolean = false

}

class PrintADAM(protected val args: PrintADAMArgs) extends ADAMSparkCommand[PrintADAMArgs] {
  val companion = PrintADAM

  def displayRaw(sc: SparkContext, file: String) {
    val it = new ParquetFileTraversable[IndexedRecord](sc, file)
    it.foreach(pileup => {
      println(pileup.toString())
    })
  }

  def displaySemantic(sc: SparkContext, file: String) {
    // This one works for any Parquet file:
    val rawTraversable = new ParquetFileTraversable[IndexedRecord](sc, file)
    rawTraversable.headOption match {
      case None => {
        println("This File is empty")
      }
      case Some(witness) => {
        val schema = witness.getSchema
        println("Schema: " + schema.getName() + " (" + schema.getType() + ")")
        schema.getName() match {
          /* Thanks to
              $ cat adam-format/src/main/resources/avro/adam.avdl | egrep '^record' | awk '{ print "case \""$2"\" => println(\"NOT IMPLEMENTED: "$2"\")" }' 
           */
          case "ADAMRecord"                   => println("NOT IMPLEMENTED: ADAMRecord")
          case "ADAMNucleotideContigFragment" => println("NOT IMPLEMENTED: ADAMNucleotideContigFragment")
          case "ADAMPileup"                   => println("NOT IMPLEMENTED: ADAMPileup")
          case "ADAMNestedPileup"             => println("NOT IMPLEMENTED: ADAMNestedPileup")
          case "ADAMContig"                   => println("NOT IMPLEMENTED: ADAMContig")
          case "ADAMVariant" => {
            val traversable = new ParquetFileTraversable[ADAMVariant](sc, file)
            traversable.foreach(row => {
            })
          }
          case "VariantCallingAnnotations"     => println("NOT IMPLEMENTED: VariantCallingAnnotations")
          case "ADAMGenotype"                  => println("NOT IMPLEMENTED: ADAMGenotype")
          case "VariantEffect"                 => println("NOT IMPLEMENTED: VariantEffect")
          case "ADAMDatabaseVariantAnnotation" => println("NOT IMPLEMENTED: ADAMDatabaseVariantAnnotation")
          case other                           => println("Unknown Schema !")
        }
      }
    }
  }
  def run(sc: SparkContext, job: Job) {
    args.filesToPrint.foreach(file => {
      if (args.displayRaw) {
        displayRaw(sc, file)
      } else {
        displaySemantic(sc, file)
      }
    })
  }
}
