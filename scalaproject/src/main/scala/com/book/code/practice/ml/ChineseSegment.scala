package com.book.code.practice.ml

import java.nio.file.Paths

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import org.apache.log4j.Logger
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.mutable.ArrayBuffer

object JiebaSegmenterSingleton {
  val logger = Logger.getLogger(getClass.getName)
  @volatile private var instance: JiebaSegmenter = null

  def getInstance(userDict: Option[Array[String]]): JiebaSegmenter = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          // 用户定义词典
          userDict match {
            case Some(files: Array[String]) => for (file <- files) {
              logger.info(s"load user dict: $file")
              WordDictionary.getInstance().loadUserDict(Paths.get(file))
            }
            case None =>
          }
          // 初始化分词器
          logger.info(s"init jieba segmenter")
          instance = new JiebaSegmenter()
        }
      }
    }

    instance
  }
}

class ChineseSegment(override val uid: String, private val userDict: Option[Array[String]])
  extends UnaryTransformer[String, Seq[String], ChineseSegment] {

  def this() = this(Identifiable.randomUID("chi"), None)

  def this(userDict: Option[Array[String]]) = this(Identifiable.randomUID("chi"), userDict)

  override def copy(extra: ParamMap): ChineseSegment = defaultCopy(extra)

  override protected def createTransformFunc: String => Seq[String] = (sentence: String) => {
    // 同义词和噪声词处理
    for ((k, v) <- ChineseSegment.replaceStr) {
      sentence.replace(k, v)
    }

    // 对于特殊的形式也做一个过滤
    ChineseSegment.reg.replaceAllIn(sentence, "")

    // 这里使用中文分词进行分词处理
    val segs = JiebaSegmenterSingleton.getInstance(userDict).process(sentence, SegMode.SEARCH).iterator()
    val words = ArrayBuffer[String]()

    while (segs.hasNext) {
      val word = segs.next().word.stripMargin(' ')
      words += word
    }

    words
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)
}

object ChineseSegment {
  private val replaceStr = Map[String, String]("斜跨包" -> "斜挎包", "!" -> ",", "！" -> ",", "。" -> ",",
    "，" -> ",", "市场价" -> "", "全国包邮" -> "", "包邮" -> "", "【" -> "", "】" -> "", "[" -> "", "]" -> "", "《" -> "", "》" -> "")

  private val reg = "仅[售][0-9.]*元".r
}