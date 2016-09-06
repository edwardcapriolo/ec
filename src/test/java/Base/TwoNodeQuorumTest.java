package Base;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class TwoNodeQuorumTest extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
  }

  @Test
  public void hello() throws InterruptedException {
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
    session.execute("USE test");
    session.execute("CREATE TABLE two (x varchar, y varchar, primary key(x))");

    PreparedStatement one = session.prepare("INSERT INTO test.two (x , y) VALUES ('3', '4')")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    PreparedStatement quorum = session.prepare("INSERT INTO test.two (x , y)   VALUES ('3', '4')")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    PreparedStatement all = session.prepare("INSERT INTO test.two (x , y)  VALUES ('3', '4')")
            .setConsistencyLevel(ConsistencyLevel.ALL);
    
    session.execute(quorum.bind());
    session.execute(one.bind());
    session.execute(all.bind());
    
    fs1.get(1).getManager().destroyAndWaitForShutdown(6);
    assertException(session, quorum);
    assertException(session, all);
    session.execute(one.bind());
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