package Base;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class AnyTest extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
  }

  @Test
  public void hello() throws InterruptedException {
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE anytest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    session.execute("USE anytest");
    session.execute("CREATE TABLE two (x varchar, y varchar, primary key(x))");

    PreparedStatement one = session.prepare("INSERT INTO anytest.two (x , y) VALUES (?, '3')")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    PreparedStatement any = session.prepare("INSERT INTO anytest.two (x , y)   VALUES (?, '3')")
            .setConsistencyLevel(ConsistencyLevel.ANY);

    assertWorks(session, any);
    assertWorks(session, one);
    
    fs1.get(1).getManager().destroyAndWaitForShutdown(6);
    Thread.sleep(5000);
    assertWorks(session, any);
    assertRoughlyHalfWork(session, one);
  }

  public void assertRoughlyHalfWork(Session session, PreparedStatement any){
    int key = 100;
    int count = 0;
    for (int i = 0 ; i<key ; i++){
      try {
        session.execute(any.bind(i + ""));
        count++;
      } catch (RuntimeException ex){
        ex.printStackTrace();
      }
    }
    Assert.assertTrue("the count was" + count, count > 40);
  }
  
  public void assertWorks(Session session, PreparedStatement any){
    int key = 100;
    int count = 0;
    for (int i = 0 ; i<key ; i++){
      session.execute(any.bind(i + ""));
      count ++;
    }
    Assert.assertEquals(key, count);
  }
  
  public void assertException(Session session, PreparedStatement ps){
    try {
      session.execute(ps.bind());
      Assert.fail("This should not have worked");
    } catch (Exception e){
      System.out.println("this did not work we expected that");
    }
  }
}
