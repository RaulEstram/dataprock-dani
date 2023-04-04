package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{EjercicioFormTag, Fifa22Tag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.EnvironmentVariables.{CutoffDateTag, NationalityIdTag}
import com.bbva.datioamproduct.fdevdatio.common.fields._
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.transformations.{MapToDataFrame, TransformationsUtils}
import com.bbva.datioamproduct.fdevdatio.utils._
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import org.apache.spark.sql.DataFrame

class IOUtilsTest extends ContextProvider with IOUtils {

  "write method" should "throw a InvalidDatasetException" in {
    val inputDf: DataFrame = config.readInputsAsMap.getFullDf

    assertThrows[InvalidDatasetException] {
      write(inputDf, config.getConfig(EjercicioFormTag))
    }
  }

  "write method" should "finis a succeed execution" in {
    val inputDf: DataFrame = config.readInputsAsMap.getFullDf

    write(inputDf, config.getConfig(Fifa22Tag))

    succeed
  }


}
