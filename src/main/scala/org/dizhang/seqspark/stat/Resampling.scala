package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix, DenseVector, shuffle}
import breeze.stats.distributions.Bernoulli
import org.apache.spark.ml.regression.LinearRegression
import org.dizhang.seqspark.assoc.Encode
import org.dizhang.seqspark.assoc.Encode.Coding
import org.dizhang.seqspark.ds.Counter.CounterElementSemiGroup.PairInt
import org.dizhang.seqspark.stat.ScoreTest.{LinearModel, LogisticModel, NullModel}
import org.dizhang.seqspark.util.InputOutput.Var
import org.dizhang.seqspark.util.UserConfig.MethodConfig

/**
  * resampling class
  *
  * residue permutation, or bootstrap
  *
  */

object Resampling {

  /**
    * the base class only resample the null model
    * convenient for the SmallSample SKAT test
    *  */
  @SerialVersionUID(7778770101L)
  final case class Base(nullModel: NullModel) extends Resampling


  /**
    * the test class is for Burden test and VT test
    * the transformer is a function transforms the
    * result of variant level score test to a statistic
    *
    *  */
  @SerialVersionUID(7778770201L)
  final case class Test(refStatistic: Double, min: Int, max: Int,
                        nullModel: NullModel, x: Encode, transformer: ScoreTest => Double) extends Resampling {

    /** if weight is learned from data, re-generate the genotype coding based on newY */
    def makeNewX(newY: DenseVector[Double]): Encode = {
      x match {
        case Encode.ErecBRV(vars, _, cov, config) =>
          Encode.ErecBRV(vars, newY, cov, config)
        case Encode.ControlsMafErecBRV(vars, controls, _, cov, config) =>
          Encode.ControlsMafErecBRV(vars, controls, newY, cov, config)
        case _ => x
      }
    }

    /** re-compute the statistic for the new null model and newX */
    def makeNewStatistic(newNullModel: NullModel, newX: Encode): Double = {
      newX.getCoding.get match {
        case Encode.Fixed(c) =>
          val st = ScoreTest(newNullModel, c)
          transformer(st)
        case Encode.VT(c) =>
          val st = ScoreTest(newNullModel, c)
          transformer(st)
        case _ => refStatistic
      }
    }

    /** pValue */
    def pCount: PairInt = {
      var res = (0, 0)
      for (i <- 0 to max) {
        if (res._1 >= min)
          return res
        else {
          val newNullModel = makeNewNullModel
          val newX = makeNewX(newNullModel.responses)
          val statistic = makeNewStatistic(newNullModel, newX)
          res = PairInt.op(res, if (statistic > refStatistic) (1, 1) else (0, 1))
        }
      }
      res
    }
  }
}
@SerialVersionUID(7778770001L)
sealed trait Resampling extends HypoTest {
  def nullModel: NullModel
  /** new null model is made based on newY,
    * which could be from permutated residuals (linear),
    * or from bootstrap (logistic)
    * */
  def makeNewNullModel: NullModel = {
    nullModel match {
      case LinearModel(y, e, c, i) =>
        val newY = e + shuffle(nullModel.residuals)
        val reg = new LinearRegression(newY, c(::, 1 until c.cols))
        LinearModel(newY, reg.estimates, c, i)
      case LogisticModel(y, e, c, i) =>
        val newY = e.map(p => if (new Bernoulli(p).draw()) 1.0 else 0.0)
        val reg = new LogisticRegression(newY, c(::, 1 until c.cols))
        LogisticModel(newY, reg.estimates, c, reg.information)
      case _ => nullModel
    }
  }
}



