package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class CourseSparkProcessTest extends ContextProvider {
  "when i execute runProcessMethod with a correct RuntimeContext" should "Return 0" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(config)
    val courseSparkProcess: CourseSparkProcess = new CourseSparkProcess

    val exitCode: Int = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe 0
  }

  it should "return exit code -1 when joinException is thrown" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configJoinException)
    val courseSparkProcess: CourseSparkProcess = new CourseSparkProcess

    val exitCode = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe -1
  }

  it should "Return exit code -1 when InvalidDatasetException is thrown" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configInvalidDatasetException)
    val courseSparkProcess: CourseSparkProcess = new CourseSparkProcess

    val exitCode = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe -1
  }
}
