import org.junit.Test

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileWritePerfTest extends FileTestBase {
  val bytes = new Array[Byte](2048).map(_ => 'x'.toByte)

  @Test
  def test1(): Unit = {
    clock {
      for (i <- 0 to 10000000) {
        if (i % 10000 == 0)
          println(i)

        writeFile(bytes)
      }
    }
  }
}
