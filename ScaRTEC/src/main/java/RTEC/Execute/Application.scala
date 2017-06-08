package RTEC.Execute

object Application extends App {

  override def main(args: Array[String]): Unit = {

    execute()

  }

  private def execute(): Unit = {

    val inputDir = "./example/maritime_brest_cyril"
    val outputFile = "./recognition.txt"
    // step
    val slidingStep = 3600
    // working memory
    val windowSize = 3600
    // start recognition from time point below
    val startTime = 1443652397 + slidingStep
    //val startTime = 1451606413 + slidingStep
    // last time
    //val lastTime = 1243825200
    val lastTime = 1459463597
    //val lastTime = 1454284813
    // dataset specific clock
    val clock = 1

    // set input directory and output file
    WindowHandler.setIOParameters(inputDir,outputFile)
    // start event recognition loop
    WindowHandler.performER(windowSize,slidingStep,lastTime,startTime,clock)

  }

}

