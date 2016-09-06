package Base;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import io.teknek.farsandra.Farsandra;

public class Base {

  public List<Farsandra> fs1 = new ArrayList<>();
  
  @Before
  public void setup() throws InterruptedException {
    //fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    //fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
  }
  
  @After
  public void close() {
    for (Farsandra fs : fs1) {
      if (fs != null) {
        try {
          fs.getManager().destroyAndWaitForShutdown(6);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
}
