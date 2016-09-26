package org.dizhang.seqspark.assoc

import breeze.linalg.{*, diag, eig, eigSym, sum, svd, DenseMatrix => DM, DenseVector => DV}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.stat.ScoreTest.{LinearModel, LogisticModel, NullModel}
import SKAT._
import org.apache.spark.SparkContext

/**
  * variance component test
  *
  * use the identical META analysis implementation
  *
  */

object SKAT {

  def apply(nullModel: NullModel,
            x: Encode,
            rho: Double = 0.0): SKAT = {
    val method = x.config.misc.getString("method")
    method match {
      case "liu.mod" => LiuModified(nullModel, x, rho)
      case "liu" => Liu(nullModel, x, rho)
      case _ => Davies(nullModel, x, rho)
    }
  }

  def apply(nullModel: LogisticModel,
            x: Encode,
            resampled: DM[Double],
            rho: Double = 0.0): SKAT = {
    SmallSampleAdjust(nullModel, x, resampled, rho)
  }

  def makeResampled(nullModel: LogisticModel)(implicit sc: SparkContext): DM[Double] = {
    val times = sc.parallelize(1 to 10000)
    val nm = sc.broadcast(nullModel)
    times.map(i =>
      Resampling.Base(nm.value).makeNewNullModel.residuals.toDenseMatrix
    ).reduce((m1, m2) => DM.vertcat(m1, m2))
  }

  def getLambdaU(sm: DM[Double]): (DV[Double], DM[Double]) = {
    val eg = eigSym(sm)
    val vals = eg.eigenvalues
    val vecs = eg.eigenvectors
    val lambda = DV(vals.toArray.filter(v => v > 1e-6): _*)
    val u = DV.horzcat(
      (for {
        i <- 0 until vals.length
        if vals(i) > 1e-6
       } yield vecs(::, i)): _*)
    (lambda, u)
  }

  @SerialVersionUID(7727750101L)
  final case class Davies(nullModel: NullModel,
                          x: Encode,
                          rho: Double = 0.0) extends SKAT {

    def pValue: Double = {
      1.0 - LCCSDavies.Simple(lambda).cdf(qScore).pvalue
    }
  }
  @SerialVersionUID(7727750201L)
  final case class Liu(nullModel: NullModel,
                       x: Encode,
                       rho: Double = 0.0) extends SKAT {

    def pValue: Double = {
      1.0 - LCCSLiu.Simple(lambda).cdf(qScore).pvalue
    }
  }
  @SerialVersionUID(7727750301L)
  final case class LiuModified(nullModel: NullModel,
                               x: Encode,
                               rho: Double = 0.0) extends SKAT {

    def pValue: Double = {
      1.0 - LCCSLiu.Modified(lambda).cdf(qScore).pvalue
    }
  }
  @SerialVersionUID(7727750401L)
  final case class SmallSampleAdjust(nullModel: LogisticModel,
                                     x: Encode,
                                     resampled: DM[Double],
                                     rho: Double = 0.0) extends SKAT {
    /** resampled is a 10000 x n matrix, storing re-sampled residuals */
    val simScores = resampled * geno
    val simQs: DV[Double] = simScores(*, ::).map(s => s.t * kernel * s)

    def pValue: Double = {
      val dis = new LCCSResampling(lambda, u, nullModel.residualsVariance, simQs)
      1.0 - dis.cdf(qScore).pvalue
    }
  }
}

/**
  * SKAT and SKAT-O are analytic tests
  * The resampling version would be too slow
  * */
@SerialVersionUID(7727750001L)
trait SKAT extends AssocMethod with AssocMethod.AnalyticTest {
  def nullModel: NullModel
  def x: Encode
  def isDefined: Boolean = x.isDefined
  def rho: Double
  lazy val weight = x.weight()
  /**
    * we trust the size is not very large here
    * otherwise, we could not store the matrix in memory
    * */
  lazy val kernel: DM[Double] = {
    val size = weight.length
    val r = (1.0 - rho) * DM.eye[Double](size) + rho * DM.ones[Double](size, size)
    diag(weight) * r * diag(weight)
  }
  lazy val geno = x.getRare().get.coding
  lazy val scoreTest: ScoreTest = ScoreTest(nullModel, geno)

  def qScore: Double = {
    scoreTest.score.t * kernel * scoreTest.score
  }
  lazy val scoreSigma = symMatrixSqrt(scoreTest.variance)
  lazy val vc = scoreSigma * kernel * scoreSigma
  lazy val (lambda, u) = getLambdaU(vc)
  def pValue: Double
  def result = AssocMethod.AnalyticResult(x.getRare().get.vars, 1.0 - pValue, pValue)
}