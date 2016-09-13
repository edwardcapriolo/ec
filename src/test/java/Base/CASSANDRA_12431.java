package Base;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import junit.framework.Assert;

public class CASSANDRA_12431 extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101"));
  }
  
  @Test
  public void hello() throws InterruptedException, IOException {
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE eventualtest");
    session.execute(new String(java.nio.file.Files.readAllBytes(new File("src/test/resources/cql.txt").toPath())));
  
    PreparedStatement quorumWrite = session.prepare("INSERT INTO eventualtest.email_histogram  (id , email,score) VALUES (?, ?, ?)")
          .setConsistencyLevel(ConsistencyLevel.QUORUM);
    String id = "thisisaguidreally";
    for (int i = 0 ; i< 20000; i ++){
      session.execute(quorumWrite.bind(id,i+"@prodigy.net",5.5f));
    }
    
    PreparedStatement read = session.prepare("SELECT * FROM eventualtest.email_histogram WHERE id=?");
    int nullCount = 0;
    for (int i = 0 ; i < 100000; i++){
      ResultSet rs = session.execute(read.bind(id));
      
      for (Row r : rs){
        Float f  = r.getFloat("score");
        if (f == null){
          nullCount ++;
          System.out.println("found a null row " + r);
        }
        if (i % 100 == 0){
          System.out.println("iteration "+i);
          System.out.println(r);
        }  
      }
      
    }
    Assert.assertEquals(0, nullCount);
  }
}