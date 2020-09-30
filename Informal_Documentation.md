# Informal Documentation

## MetaAnalysis.scala Code
Sam's understanding: this code is creating a MetaAnalysis object that sends logger informational messages of the normal operational statuses.
The following line of code:
```scala
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
````
creates a logger variable that is used to send info-level logger message. You can also send error, warning, debug, and trace messages. Getting an instance of logger only requires the import which is done in the beginning of the code and the fucntion call `Loggerfactory.getLogger`. This function takes in string of class on which it is being called on.
The following code:
```scala
  def apply(seqContext: SeqContext): Unit = {
      logger.info("start meta analysis")

      /** Spark configuration */

      val mm = new MetaMaster(seqContext)

      mm.run()

      logger.info("end meta analysis")
  }

 }
}
```
first creates a an apply function that has a void return type. Logger sends an informational message conveying the start of the analysis. Within the procedure, `seqContext` is used to create a new MetaMaster object stored in var `mm`. The `run` function is then called on `mm` to execute some calculations on mm (I think, please clarify if not!). Logger then sends an end of analysis message.

