package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}
import com.datio.dataproc.sdk.launcher.SparkLauncher

class CourseLauncherTest extends ContextProvider {

  "SparkLauncher execute" should "return 0 in success execution" in {
    val args: Array[String] = Array("src/test/resources/config/CourseTest.conf", "CourseSparkProcess")
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 0
  }

  it should "return 1 when Fatal dataproc error is thrown" in {
    val args: Array[String] = Array("src/test/resources/config/CourseJoinExceptionTest.conf", "CourseSparkProcess")
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 1
  }

}
