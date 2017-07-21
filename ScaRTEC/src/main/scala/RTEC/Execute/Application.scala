package RTEC.Execute

import org.apache.log4j.varia.NullAppender

object Application extends App {

  override def main(args: Array[String]): Unit = {

    execute(args)

  }

  private def execute(args: Array[String]): Unit = {

    // disable log4j warnings for now
    org.apache.log4j.BasicConfigurator.configure(new NullAppender)

    val inputDir = "./patterns/"
    val outputFile = "./recognition.json"
    // step
    val slidingStep = args.head.toLong
    // working memory
    val windowSize = args(1).toLong
    // dataset specific clock
    val clock = 1L

    // set input directory and output file
    WindowHandler.setIOParameters(inputDir,outputFile)
    // start event recognition loop
    WindowHandler.performER(windowSize,slidingStep,clock)

  }

}

