// Raúl de Jesús Estrada Zermeño

package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.InputTag
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.EnvironmentVariables.{CutoffDateTag, LeagueNameTag, NationalityIdTag}
import com.bbva.datioamproduct.fdevdatio.transformations.ReplaceColumnException
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.convert.ImplicitConversions.`set asScala`


package object utils {

  case class Params(nationalityID: Int, cutoffDate: String, leagueName: String)

  implicit class SuperConfig(config: Config) extends IOUtils {
    def readInputsAsMap: Map[String, DataFrame] = {
      config.getObject(InputTag).keySet().map(
        key => {
          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          (key, read(inputConfig))
        }
      ).toMap
    }

    def getParams: Params = Params(
      nationalityID = config.getInt(NationalityIdTag),
      cutoffDate = config.getString(CutoffDateTag),
      leagueName = config.getString(LeagueNameTag)
    )

  }

  implicit class DataframesUtils(dataFrame: DataFrame) {
    def getMax(column: Column): Int = {
      dataFrame // dataframe que se utiliza para llamar a este método
        .select(max(column)) // Type -> DataFrame
        .head() // Type -> Row
        .getInt(0) // type -> Int
    }

    def filterByNationalityId(nationality_id: Int): DataFrame = {
      if (nationality_id == -1) dataFrame else dataFrame.filter(col("nationality_id") === nationality_id)
    }

    def replaceColumn(field: Column): DataFrame = {
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name

      if (!dataFrame.columns.contains(columnName)) throw ReplaceColumnException(
        s"La columna '$columnName', no puede ser remplazada.", columnName, dataFrame.columns)

      val columns: Array[Column] = dataFrame.columns.map(nombre => {
        if (nombre != columnName) col(nombre) else field
      })
      dataFrame.select(columns: _*)
    }
  }

}