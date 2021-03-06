package Base;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class MvEventual3Node extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "507", "127.0.0.101", "3.9"));
    fs1.add(Util.build("127.0.0.102", "508", "127.0.0.101", "3.9"));
    fs1.add(Util.build("127.0.0.103", "509", "127.0.0.101", "3.9"));
  }
  
  String keyspace = "mvks";
  String tablename = "mveventualtest";
  String mv = "mv1";
  
  @Test
  public void hello() throws InterruptedException {
    Thread.sleep(30000);
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE "+ keyspace +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
    session.execute("USE " + keyspace );
    session.execute("CREATE TABLE "+tablename+" (x varchar, y varchar, z varchar, z1 varchar, primary key(x, y))");
    session.execute( "CREATE MATERIALIZED VIEW mv1 AS " +
    " SELECT x,y,z,z1 FROM " + tablename +
    " WHERE x is not null and y is not null " +
     "PRIMARY KEY (x,y) " +
    " WITH CLUSTERING ORDER BY (z desc) ");
    
    {
      PreparedStatement quorumWrite = session.prepare("INSERT INTO "+ keyspace +"."+tablename+" (x , y, z, z1) VALUES (?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
      PreparedStatement quorumMvRead = session.prepare("SELECT * FROM "+ keyspace +"."+mv+"  where x=?")
              .setConsistencyLevel(ConsistencyLevel.QUORUM);
      assertQuorumQuorumIsConsistent(session, quorumWrite,quorumMvRead);
    }

  }
  
  public void assertQuorumQuorumIsConsistent(Session session, PreparedStatement quorumWrite,  
          PreparedStatement quorumMvRead){
    for (int i = 0; i < 10000; i++) {
      session.execute(quorumWrite.bind(i+"", i+1+"", i+2+"",i+3+"" ));
      int mvsize = session.execute(quorumMvRead.bind(i+"")).all().size();
      Assert.assertEquals(1, mvsize);
    }
  }
  
}
