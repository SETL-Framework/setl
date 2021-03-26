package io.github.setl.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ConfLoaderSuite extends AnyFunSuite {

  test("ConfigLoader builder should build ConfigLoader") {
    System.setProperty("app.environment", "test")
    System.setProperty("myvalue", "test-my-value")

    val cl = ConfigLoader.builder()
      .setAppEnv("local")
      .setAppName("TestConfigLoaderBuilder")
      .setProperty("myJvmProperty", "myJvmPropertyValue")
      .getOrCreate()

    assert(cl.get("test.string") === "foo")
    assert(cl.get("test.variable") === "myJvmPropertyValue")
    assert(cl.appName === "TestConfigLoaderBuilder")

    System.clearProperty("app.environment")
    System.clearProperty("myvalue")
  }

  test("Getters of ConfigLoader") {
    System.setProperty("app.environment", "test")
    val cl = ConfigLoader.builder()
      .setAppEnv("local")
      .setConfigPath("test_priority.conf")
      .getOrCreate()

    assert(cl.get("my.value") === "haha")
    assert(cl.getOption("my.value") === Some("haha"))
    assert(cl.getOption("notExisting") === None)
    assert(cl.getArray("test.list") === Array("1","2","3"))
    assert(cl.getObject("setl.config") === cl.config.getObject("setl.config"))
  }

  test("ConfigLoader builder should prioritize setConfigPath than setAppEnv and jvm property and pom") {
    System.setProperty("app.environment", "test")
    val cl = ConfigLoader.builder()
      .setAppEnv("local")
      .setConfigPath("test_priority.conf")
      .getOrCreate()

    assert(cl.get("my.value") === "haha")
    System.clearProperty("app.environment")
  }

  test("ConfigLoader builder should take into account the app.environment property in pom") {
    System.clearProperty("app.environment")
    val configLoader = ConfigLoader.builder().getOrCreate()
    assert(configLoader.appEnv === ConfigFactory.load().getString("setl.environment"))
    System.clearProperty("app.environment")
  }

  test("ConfigLoader builder should prioritize setAppEnv than jvm property and pom") {
    System.setProperty("app.environment", "test")

    val cl = ConfigLoader.builder()
      .setAppEnv("test_priority")
      .getOrCreate()

    assert(cl.get("my.value") === "haha")
    System.clearProperty("app.environment")
  }

  test("ConfigLoader builder should prioritize jvm property than pom") {
    System.setProperty("app.environment", "test_priority")

    val cl = ConfigLoader.builder()
      .getOrCreate()

    assert(cl.get("my.value") === "haha")
    System.clearProperty("app.environment")
  }
}
