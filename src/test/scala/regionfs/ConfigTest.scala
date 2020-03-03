package regionfs

import java.io.File

import org.grapheco.regionfs.GlobalConfigWriter
import org.junit.Test

/**
  * Created by bluejoe on 2020/3/3.
  */
class ConfigTest {
  @Test
  def test(): Unit = {
    new GlobalConfigWriter().write(new File("./global.conf"));
  }
}
