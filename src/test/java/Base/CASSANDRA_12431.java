package Base;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
  public void hello() throws InterruptedException, IOException, ExecutionException {
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE eventualtest");
    session.execute(new String(java.nio.file.Files.readAllBytes(new File("src/test/resources/cql.txt").toPath())));
    PreparedStatement quorumWrite = session.prepare("INSERT INTO eventualtest.email_histogram  (id , email,score) VALUES (?, ?, ?)")
          .setConsistencyLevel(ConsistencyLevel.QUORUM);
    String id1 = "thisisaguidreally";
    String id2 = "thisisaguidreally1";
    for (int i = 0 ; i< 20000; i ++){
      session.execute(quorumWrite.bind(id1,i+"@prodigy.net",5.5f));
    }
    for (int i = 0 ; i< 20000; i ++){
      session.execute(quorumWrite.bind(id2,i+"@prodigy.edu",4.5f));
    }
    
    PreparedStatement read = session.prepare("SELECT * FROM eventualtest.email_histogram WHERE id=?");
    
    ExecutorService e = Executors.newFixedThreadPool(100);
    List<Future<Long>> res = e.invokeAll(Arrays.asList( new NullCounter(session, read, id1), 
            new NullCounter(session, read, id1),
            new NullCounter(session, read, id2),
            new NullCounter(session, read, id2)
            ));
    long sum = 0;
    for (Future<Long> i : res){
      sum += i.get();
    }
    Assert.assertEquals(0, sum);
  }
  
  public static class NullCounter implements Callable<Long> {

    private Session session;
    private PreparedStatement read;
    private String id;
    public NullCounter(Session session, PreparedStatement read, String id){
      this.session = session;
      this.read = read;
      this.id = id;
    }
    
    
    @Override
    public Long call() throws Exception {
      int nullCount = 0;
      for (int i = 0 ; i < 10; i++){
        ResultSet rs = session.execute(read.bind(id));          
        for (Row r : rs){
          if (r.isNull("score")){
            nullCount ++;
            System.out.println("found a null row " + r);
          }
        }
        if (i % 100 == 0){
          System.out.println("iteration "+i);
        }  
      }
      return (long) nullCount;
    }
    
  }
}