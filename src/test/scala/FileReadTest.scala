import java.io.File

import org.junit.Test

/**
  * Created by bluejoe on 2020/1/8.
  */
class FileReadTest extends FileTestBase {
  @Test
  def test(): Unit = {
    val id = super.writeFile(new File("./testdata/inputs/999"));
    clock {
      val file = client.readFile(id);
      client.readFile(id);
      client.readFile(id);
    }
  }
}
