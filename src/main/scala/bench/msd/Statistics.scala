package bench.msd

import scala.math.pow
import scala.math.sqrt
/**
  * Reproduced with permission from Austin Poulton from group BDP coursework, 2015
  */


/**
  * Sum components tuple
  * @param xSum
  * @param ySum
  * @param xySum
  * @param xxSum
  * @param yySum
  */
case class SumComp(xSum: Double = 0.0, ySum : Double = 0.0, xySum : Double = 0.0, xxSum : Double = 0.0 , yySum : Double = 0.0, n : Int = 0) extends Serializable
{

  def +(that: SumComp): SumComp
          = SumComp(this.xSum+that.xSum, this.ySum + that.ySum, this.xySum + that.xySum, this.xxSum + that.xxSum, this.yySum+that.yySum, this.n + that.n)

  override def toString() = "x Sum:\t"+xSum+"\ty Sum:\t"+ySum+"\txy Sum:\t"+xySum+"\tx^2 Sum:\t"+xxSum+"\ty^2 Sum:\t"+yySum+"\tn:\t"+n
}

/**
  * Generals statistic operations
  */
trait Statistics {


  def spearmanAggregator(acc : SumComp, x: Double, y: Double):SumComp
                    = SumComp(acc.xSum+x,acc.ySum+y,acc.xySum+x*y, acc.xxSum + pow(x,2), acc.yySum+pow(y,2), acc.n + 1)

  def spearmanCombiner(acc1: SumComp, acc2: SumComp):SumComp =  acc1 + acc2


  def spearmanCorrelation(acc: SumComp): Double = {
     println(acc)
    (acc.n * acc.xySum - acc.xSum * acc.ySum) / (sqrt(acc.n * acc.xxSum - pow(acc.xSum, 2)) * sqrt(acc.n * acc.yySum - pow(acc.ySum, 2)))
  }
}
