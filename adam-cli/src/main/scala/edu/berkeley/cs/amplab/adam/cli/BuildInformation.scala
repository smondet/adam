/*
 * Copyright (c) 2014. Sebastien Mondet <seb@mondet.org>
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
package edu.berkeley.cs.amplab.adam.cli

import org.kohsuke.args4j.{Option => Args4jOption, Argument}
import scala.collection.JavaConversions._
import scala.Some
import scala.collection.JavaConverters._
import java.util.Properties

object BuildInformation extends AdamCommandCompanion {
  val commandName: String = "buildinformation"
  val commandDescription: String = "Display build information (use this for bug reports)"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new BuildInformation(Args4j[BuildInformationArgs](cmdLine))
  }
}

class BuildInformationArgs extends Args4jBase with ParquetArgs {
  // @Args4jOption(required = false, name = "-output", usage = "Output to a file instead of <stdout>")
  // var outputFile: String = null
}

class BuildInformation(args: BuildInformationArgs) extends AdamCommand {
  val companion = BuildInformation

  def run() = {

    val properties = new Properties;
    properties.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
    val propertiesMap = properties.asScala; 
    val asAString =
        propertiesMap.foldLeft(""){
            case (prev_string, (key, value)) =>
                prev_string + key + ": " + value + "\n"
        };
    // println("Hello from BuildInformation: output: " + args.outputFile
    // + properties.get("git.branch") )
    println("Build information:\n" + asAString);

  }


}
