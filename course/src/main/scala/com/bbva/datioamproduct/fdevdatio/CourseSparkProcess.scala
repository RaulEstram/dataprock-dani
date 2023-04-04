package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{EjercicioFormTag, Fifa22Tag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.EnvironmentVariables.{CutoffDateTag, NationalityIdTag}
import com.bbva.datioamproduct.fdevdatio.common.fields._
import com.bbva.datioamproduct.fdevdatio.utils.DataframesUtils
import com.bbva.datioamproduct.fdevdatio.transformations.{JoinException, MapToDataFrame, ReplaceColumnException, TransformationsUtils}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, Params, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}


class CourseSparkProcess extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    Try {
      val config: Config = runtimeContext.getConfig
      val params: Params = config.getParams
      val dataFrames: Map[String, DataFrame] = config.readInputsAsMap

      val dataFrame: DataFrame = dataFrames.getFullDf
        .filterByNationalityId(config.getInt(NationalityIdTag))
        .replaceColumn(PlayerPositions())
        .replaceColumn(PlayerTraits())
        .addColumn(CutoffDate(config.getString(CutoffDateTag)))
        .addColumn(CatAge())
        .addColumn(ZScore())

      dataFrame.show()

      write(dataFrame, config.getConfig(EjercicioFormTag))

    } match {
      case Success(_) => 0
      case Failure(exception: ReplaceColumnException) =>
        logger.error(exception.getMessage)
        logger.error(s"Columna a remplazar: ${exception.columnName}")
        logger.error(s"Columnas encontradas: [${exception.columnas.mkString(", ")}]")
        -1
      case Failure(exception: JoinException) =>
        logger.error("Sucedió un Error :c")
        logger.error(s"message: ${exception.message}")
        logger.error(s"Localización del error: ${exception.location}")
        logger.error(s"la primary key esperada fue: ${exception.expectedKeys.mkString(", ")}")
        logger.error(s"Las columnas del Dataframe encontrados fueron: ${exception.columns.mkString(", ")}")
        -1
      case Failure(exception: InvalidDatasetException) =>
        logger.error("InvalidDataset hola")
        exception.getErrors.forEach(error => logger.error(error.toString))
        -1
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        -1
    }
  }


  override def getProcessId: String = "CourseSparkProcess"
}
