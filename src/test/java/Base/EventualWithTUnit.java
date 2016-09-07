package Base;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import io.teknek.tunit.TUnit;

public class EventualWithTUnit extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101"));
  }
  
  @Test
  public void test(){
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE eventualtest2");
    session.execute("CREATE TABLE three (x varchar, y varchar, primary key(x))");
    
    PreparedStatement oneWrite = session.prepare("INSERT INTO eventualtest2.three (x , y)   VALUES (?, '3')")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    PreparedStatement oneRead = session.prepare("SELECT * FROM eventualtest2.three   WHERE x=?")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    assertConsistency(session, oneWrite, oneRead);
  }
  
  public void assertConsistency(Session session, PreparedStatement oneWrite, PreparedStatement oneRead){
    int total = 0;
    for (int i = 10000; i < 20000; i++) {
      final int j = i;
      session.execute(oneWrite.bind(i + ""));
      TUnit.assertThat(() ->  session.execute(oneRead.bind(j + "")).all().size())
        .afterWaitingAtMost(5, TimeUnit.SECONDS).isEqualTo(1); 
      total += 1;
    }
    Assert.assertEquals(10000, total);
  }
  
  
}
