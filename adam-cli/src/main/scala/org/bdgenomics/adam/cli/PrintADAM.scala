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

import org.apache.avro.generic.{ GenericDatumWriter, IndexedRecord }
import scala.collection.JavaConversions._
import org.bdgenomics.adam.util.ParquetFileTraversable
import org.eclipse.jetty.io.UncheckedPrintWriter
import com.twitter.chill.Base64.OutputStream

// import org.apache.spark.api.java.*

import java.util
import java.io.PrintWriter
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.avro.data.Json
import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMVariant, ADAMRecord }
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

  @Args4jOption(required = false, name = "-o", metaVar = "FILE", usage = "Output to a (local) file")
  var outputFile: String = _

  @Args4jOption(required = false, name = "-pretty", usage = "Display raw, pretty-formatted JSON")
  var prettyRaw: Boolean = false
}

class PrintADAM(protected val args: PrintADAMArgs) extends ADAMSparkCommand[PrintADAMArgs] {
  val companion = PrintADAM

  def withPrintStream(f: Option[String])(op: java.io.PrintStream => Unit) {
    val p = f match {
      case Some(s) => new java.io.PrintStream(s)
      case None    => System.out
    }
    try { op(p) } finally { p.close() }
  }

  def displayRaw(sc: SparkContext, file: String, output: Option[String]) {
    withPrintStream(output)(out => {
      val pretty = args.prettyRaw
      val it = new ParquetFileTraversable[IndexedRecord](sc, file)
      pretty match {
        case true => {
          it.headOption match {
            case None => out.print("")
            case Some(hd) =>
              val schema = hd.getSchema()
              val writer = new GenericDatumWriter[Object](schema)
              val encoder = EncoderFactory.get().jsonEncoder(schema, out)
              val jg = (new org.codehaus.jackson.JsonFactory()).createJsonGenerator(out)
              jg.useDefaultPrettyPrinter()
              encoder.configure(jg)
              it.foreach(pileup => {
                writer.write(pileup, encoder)
              })
          }
        }
        case false => {
          it.foreach(pileup => {
            out.println(pileup.toString())
          })
        }
      }
    })
  }

  //  def displayADAMGenotype(sc: SparkContext, file: String, out: PrintWriter) {
  //    val traversable = new ParquetFileTraversable[ADAMGenotype](sc, file)
  //    traversable.foreach(row => {
  //      out.println("-----------------------")
  //      val variant = row.getVariant
  //      out.println("Variant:")
  //      out.println("  Contig: " + variant.contig)
  //      out.println("  Position: " + variant.position)
  //      out.println("  Reference Allele: " + variant.referenceAllele)
  //      out.println("  Variant Allele: " + variant.variantAllele)
  //      var annotations = row.variantCallingAnnotations
  //      out.println("Variant Calling Annotations")
  //      out.println("  Depth: " + annotations.readDepth +
  //        " ")
  //
  //    })
  //  }
  //
  //  def displaySemantic(sc: SparkContext, file: String, output: Option[String]) {
  //    withPrintWriter(output)(out => {
  //      // This one works for any Parquet file:
  //      val rawTraversable = new ParquetFileTraversable[IndexedRecord](sc, file)
  //      rawTraversable.headOption match {
  //        case None =>
  //          out.println("This File is empty")
  //        case Some(witness) => {
  //          val schema = witness.getSchema
  //          out.println("Schema: " + schema.getName() + " (" + schema.getType() + ")")
  //          schema.getName() match {
  //            // Avro does not generate a parent “case class” so we have to this ugly string matching:
  //            case "ADAMRecord" =>
  //              println("NOT IMPLEMENTED: ADAMRecord")
  //            case "ADAMNucleotideContigFragment" =>
  //              println("NOT IMPLEMENTED: ADAMNucleotideContigFragment")
  //            case "ADAMPileup" =>
  //              println("NOT IMPLEMENTED: ADAMPileup")
  //            case "ADAMNestedPileup" =>
  //              println("NOT IMPLEMENTED: ADAMNestedPileup")
  //            case "ADAMContig" =>
  //              println("NOT IMPLEMENTED: ADAMContig")
  //            case "ADAMVariant" => {
  //              val traversable = new ParquetFileTraversable[ADAMVariant](sc, file)
  //              traversable.foreach(row => {
  //              })
  //            }
  //            case "VariantCallingAnnotations" =>
  //              println("NOT IMPLEMENTED: VariantCallingAnnotations")
  //            case "ADAMGenotype" => displayADAMGenotype(sc, file, out)
  //            case "VariantEffect" =>
  //              println("NOT IMPLEMENTED: VariantEffect")
  //            case "ADAMDatabaseVariantAnnotation" =>
  //              println("NOT IMPLEMENTED: ADAMDatabaseVariantAnnotation")
  //            case other =>
  //              println("Unknown Schema !")
  //          }
  //        }
  //      }
  //    })
  //  }
  def run(sc: SparkContext, job: Job) {
    val output = Option(args.outputFile)
    args.filesToPrint.foreach(file => {
      if (args.displayRaw) {
        displayRaw(sc, file, output)
      } else {
        //        displaySemantic(sc, file, output)
      }
    })
  }
}
