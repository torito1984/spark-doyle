package es.dmr.doyle.spark.locations

import org.apache.spark.SparkContext

/**
 * Created by hadoop on 8/27/15.
 */

object DoyleLocationsJob {

  def main(args: Array[String]) {

    // Run the tweet processing
    DoyleLocationsProcess.execute(
      master    = None,
      args      = args.toList,
      jars      = List(SparkContext.jarOfObject(this).get)
    )

    // Exit with success
    System.exit(0)
  }
}
