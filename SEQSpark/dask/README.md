# Migration Notes
# Overview
Data Movement to Dask: Issue #14. Identify the complete list of variables that act as inputs to association testing methods post filtration based on trait.

> Why wo do association, the foolowing is from documentation

Although it looks lengthy at first, the structure is clear and simple. First, change to the pipeline list to include association. The added  association section consists of two subsections, trait and method. 

trait describes the phenotypes to be analyzed and the corresponding covariates to include in the regression model.  It needs to be specified if the trait is binary (false would denote that the trait is quantitative), and how many principal components to include in the regression analysis. Here a binary trait disease and a quantitative trait BMI are being analyzed. 

method describes the methods to use. For single variant associaton analysis  snv is used to denote the type of analysis which should be performed. For the single variant association analysis currently the additive test is supported.

# Description
According to the result of running Hyun's demo.conf, the two output folder is in **demo/pca.csv** and **demo/qc.txt**. My task is to examine, before these two files put into the trait in association test, what should be other parameters passed into that for dask movement. Do make an exhaustive list covering all possible methods that exist in the current SEQSpark system.

# In the function

## Section 1
According to the **seqspark.log**, the very first log associated with assocation is 
```
20/11/19 20:34:47 INFO assoc.AssocMaster: we have traits: bmi,disease
```
By searching "we have traits", this is a logger.info in ```src/main/AssocMaster.scala```

```scala
@SerialVersionUID(105L)
class AssocMaster[A: Genotype](genotype: Data[A])(implicit ssc: SeqContext) {
  val cnf = ssc.userConfig
  val sc = ssc.sparkContext
  val phenotype = Phenotype("phenotype")(ssc.sparkSession)

  val logger = LoggerFactory.getLogger(this.getClass)

  val assocConf = cnf.association

  def run(): Unit = {
    val traits = assocConf.traitList
    logger.info("we have traits: %s" format traits.mkString(","))
    traits.foreach{t => runTrait(t)}
  }
```

Then, each trait is run by
```scala
def runTrait(traitName: String): Unit = {
   // try {
    logger.info(s"load trait $traitName from phenotype database")
    phenotype.getTrait(traitName) match {
      case Left(msg) => logger.warn(s"getting trait $traitName failed, skip")
      case Right(tr) =>
        val currentTrait = (traitName, tr)
        val methods = assocConf.methodList
        val indicator = sc.broadcast(phenotype.indicate(traitName))
        val controls = if (phenotype.contains(Pheno.control)) {
          Some(phenotype.select(Pheno.control).zip(indicator.value)
            .filter(p => p._2).map(p => if (p._1.get == "1") true else false))
        } else {
          None
        }

        val chooseSample = genotype.samples(indicator.value)(sc)
        val cond = LogicalParser.parse("informative")
        val currentGenotype = chooseSample.variants(cond, None, true)(ssc)
        currentGenotype.persist(StorageLevel.MEMORY_AND_DISK)
        val traitConfig = assocConf.`trait`(traitName)
        val cov =
          if (traitConfig.covariates.nonEmpty || traitConfig.pc > 0) {
            val all = traitConfig.covariates ++ (1 to traitConfig.pc).map(i => s"_pc$i")
            phenotype.getCov(traitName, all, Array.fill(all.length)(0.0)) match {
              case Left(msg) =>
                logger.warn(s"failed getting covariates for $traitName, nor does PC specified. use None")
                None
              case Right(dm) =>
                logger.debug(s"COV dimension: ${dm.rows} x ${dm.cols}")
                Some(dm)
            }
          } else
            None
        val currentCov = cov
        methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m)(sc, cnf))
    }
  }
```
the **assocConf.traitList** should be ["bmi", "disease"]

Judging from the code above ```def run```, there are several things that is useful:
- genotype: type Data[A] (is it Genotype?), function parameter
- val phenotype: Phenotype
- ssc: seqContext, function parameter

# section 2: what is seqContext, phenotype, geneotype

seqContext is under ```src/main/scala/.../util/seqContext.scala```

```scala
case class SeqContext(userConfig: RootConfig,
                      sparkContext: SparkContext,
                      sparkSession: SparkSession)
```
For me, I am not sure what is sparkContext and what is sparkSession. the userConfig file should be as the name suggests. Config file is parsed in ```src/main/scala/.../seqspark.scala```

Phenotype is under ```src/main/scala/.../ds/Phenotype.scala```

```scala
   def apply(pc: PhenotypeConfig, table: String)(spark: SparkSession): Phenotype = {

     if (pc.path.isEmpty || pc.samples == Samples.none) {
       Dummy
     } else {
       logger.info(s"creating phenotype dataframe from ${pc.path}")

       val dataFrame = spark.read.options(options).csv(pc.path)

       dataFrame.createOrReplaceTempView(table)

       Distributed(dataFrame)
     }
  }

   def apply(table: String)(spark: SparkSession): Phenotype = {
     if (spark.catalog.tableExists("phenotype"))
       Distributed(spark.table(table))
     else
       Dummy
   }
```
For this one, it is config by the sparksession


We can get a sense of how genotype is called in ```src/main/scala.../worker/Pipeline.scala```:
```scala
  val a: String = "a"
  val b: Byte = 'b'
  val c: Imp = (0, 0, 0)

  def apply(implicit ssc: SeqContext): Unit = {
    val conf = ssc.userConfig
    conf.input.genotype.format match {
      case CV.GenotypeFormat.vcf =>
        run[String, Byte](a, b)
      case CV.GenotypeFormat.imputed =>
        run[Imp, Imp](c, c)
      case CV.GenotypeFormat.cacheVcf =>
        run[Byte, Byte](b, b)
      case CV.GenotypeFormat.cacheImputed =>
        run[Imp, Imp](c, c)
    }
  }
```

More detailed flow will be discussed in the next session

## section 3: How AssocMaster is called
In ```src/main/scala/.../seqspark.scala```
```scala
// src/main/seqspark.scala
object SeqSpark {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {



    /** check args */
    if (badArgs(args)) {
      logger.error(s"bad argument: ${args.mkString(" ")}")
      System.exit(1)
    }

    val rootConf = readConf(args(0))


    val warehouse = if (rootConf.local)
      s"file://${Paths.get("").toAbsolutePath.toString}"
    else
      s"hdfs:///user/${System.getProperty("user.name")}"

    val ss: SparkSession = SparkSession
      .builder()
      .appName("SEQSpark")
      .config("spark.sql.warehouse", warehouse)
      .getOrCreate()

    val sc = ss.sparkContext

    val seqContext = SeqContext(rootConf, sc, ss)

    if (rootConf.pipeline.contains("meta")) {
      MetaAnalysis(seqContext)
    } else {
      SingleStudy(seqContext)
    }
```

this should be the place where ```src/main/scala/.../singleStudy.scala``` gets called becasue it does not have "meta"


```scala
// src/main/SingleStudy.scala
object SingleStudy {

  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(seqContext: SeqContext) {

    /** quick run */

    try {

      if (checkConf(seqContext.userConfig)) {
        run(seqContext)
      } else {
        logger.error("Configuration error, exit")
      }

    } catch {
      case e: Exception => {
        logger.error("Something went wrong, exit")
        e.printStackTrace()
      }
    }
  }
```

and the run function is 
```scala
 def run(seqContext: SeqContext) {

    val cnf = seqContext.userConfig

    val project = cnf.project

    val ss = seqContext.sparkSession

    val sc = seqContext.sparkContext

    Phenotype(cnf.input.phenotype, "phenotype")(ss)

    implicit val ssc = seqContext
    Pipeline(ssc)
 }
```

The Pipeline class is in ```src/main/scala.../worker/Pipeline.scala```. can perform your analysis as follows: 

```scala
  val a: String = "a"
  val b: Byte = 'b'
  val c: Imp = (0, 0, 0)

  def apply(implicit ssc: SeqContext): Unit = {
    val conf = ssc.userConfig
    conf.input.genotype.format match {
      case CV.GenotypeFormat.vcf =>
        run[String, Byte](a, b)
      case CV.GenotypeFormat.imputed =>
        run[Imp, Imp](c, c)
      case CV.GenotypeFormat.cacheVcf =>
        run[Byte, Byte](b, b)
      case CV.GenotypeFormat.cacheImputed =>
        run[Imp, Imp](c, c)
    }
  }
```
Note that .vcf file is genotype file per the document statement:

```
In the configuration file, specify the paths of the input genotype file (VCF) and a phenotype file (TSV), and the pipeline with the parameters you want to run. With the configuration file and all the necessary input files ready, you 
```
The association code is called by
```scala
else {
        val clean =
          if (pipeline.nonEmpty && pipeline.head == "qualityControl") {
            pipeline = pipeline.tail
            QualityControl(annotated, a, b)
          } else {
            QualityControl(annotated, a, b, pass = true)
          }

        if (pipeline.isEmpty) {
          Export(clean)
        } else {
          if (pipeline.nonEmpty && pipeline.head == "association") {
            pipeline = pipeline.tail
            Association(clean)
          }
          Export(clean)
        }
      }
    }
```

Association is in ```src/main/scala.../worker/Association.scala```

```scala
object Association {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply[B: Genotype](input: Data[B])(implicit ssc: SeqContext): Unit = {
    if (ssc.userConfig.pipeline.contains("association"))
      if (input.isEmpty()) {
        logger.warn(s"no variants left. cannot perform association analysis")
      } else {
        new AssocMaster(input).run()
      }
  }
}
```
It will call the **AssocMaster** as section 1 describes

# Draft conclusion
I think the three variables:
- genotype: type Data[A] (is it Genotype?), function parameter
- val phenotype: Phenotype
- ssc: seqContext, function parameter

They are all associated with **SeqContext**
