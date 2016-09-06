package Base;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * java.lang.AssertionError: expected:<10000> but was:<9974>
 *
 */
public class EventualDetector extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101"));
  }
  
  @Test
  public void hello() throws InterruptedException {
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE eventualtest");
    session.execute("CREATE TABLE three (x varchar, y varchar, primary key(x))");

    {
      PreparedStatement quorumWrite = session.prepare("INSERT INTO eventualtest.three (x , y) VALUES (?, '3')")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
      PreparedStatement quorumRead = session.prepare("SELECT * FROM eventualtest.three where x=?")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
      assertQuorumQuorumIsConsistent(session, quorumWrite, quorumRead);
    }
    
    {
      PreparedStatement oneWrite = session.prepare("INSERT INTO eventualtest.three (x , y)   VALUES (?, '3')")
              .setConsistencyLevel(ConsistencyLevel.ONE);
      PreparedStatement oneRead = session.prepare("SELECT * FROM eventualtest.three   WHERE x=?")
              .setConsistencyLevel(ConsistencyLevel.ONE);
      assertConsistency(session, oneWrite, oneRead);
    }

  }
  
  public void assertQuorumQuorumIsConsistent(Session session, PreparedStatement quorumWrite, PreparedStatement quorumRead){
    for (int i = 0; i < 10000; i++) {
      session.execute(quorumWrite.bind(i+""));
      int size = session.execute(quorumRead.bind(i+"")).all().size();
      Assert.assertEquals(1, size);
    }
  }
  
  public void assertConsistency(Session session, PreparedStatement oneWrite, PreparedStatement oneRead){
    int total = 0;
    for (int i = 10000; i < 20000; i++) {
      session.execute(oneWrite.bind(i+""));
      total  += session.execute(oneRead.bind(i+"")).all().size();
    }
    Assert.assertEquals(10000, total);
  }
  
}
