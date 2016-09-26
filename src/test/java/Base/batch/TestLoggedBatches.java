package Base.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import Base.Base;
import Base.Util;
import io.teknek.farsandra.Farsandra;

/*
 * writePass: 241
 * writeFail: 758
 * foundOnFirstRead: 201   
 * foundOnRetry: 24 
 */
public class TestLoggedBatches extends Base {
  
  public static int columnsPerPartition = 200;
  public static AtomicLong writePass = new AtomicLong(0);
  public static AtomicLong writeFail= new AtomicLong(0);
  
  public static AtomicLong foundOnFirstRead= new AtomicLong(0);
  public static AtomicLong foundOnRetry= new AtomicLong(0);
  
  @Before
  public void setup() throws InterruptedException {
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.101", "101", "127.0.0.101", "2.2.6"));
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.102", "102", "127.0.0.101", "2.2.6"));
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.103", "103", "127.0.0.101", "2.2.6"));
  }
  
 
  public static void insert(BatchStatement.Type statementType, String keybase, Session session, PreparedStatement ps){
    session.execute("USE eventualtest");
    int numberOfPartitions = 3;
    BatchStatement bs = new BatchStatement(statementType);
    bs.setIdempotent(true);
    bs.setConsistencyLevel(ConsistencyLevel.ONE);
    for (int i = 0; i < numberOfPartitions ; i++){
      for (int j = 0 ; j < columnsPerPartition; j++){
        bs.add(ps.bind(keybase + i +"", j + "", j + "3"));  
      }
    }
    session.execute(bs);
  }

  @Test
  public void aTest() throws InterruptedException, ExecutionException{
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("CREATE TABLE eventualtest.three (x varchar, y varchar, z varchar, primary key(x,y))");
    PreparedStatement read = session.prepare("SELECT * FROM eventualtest.three WHERE x=?")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    PreparedStatement insert = session.prepare("INSERT INTO eventualtest.three (x,y,z) VALUES (?,?,?)")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    
    ExecutorService es = Executors.newFixedThreadPool(200);
    
    List<Callable<Boolean>> writeReadCallable = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      writeReadCallable.add(new WriteReadCallable(session, i, read, insert));
    }
    writeReadCallable.set(350, faultInExecutor(fs1));
    List<Future<Boolean>> d = es.invokeAll(writeReadCallable);
    
    printStatus();
    assertCorrectness(d);
  }  

  private static Callable<Boolean> faultInExecutor(List<Farsandra> fs1){
    Callable<Boolean> x = () -> {
      fs1.get(1).getManager().destroyAndWaitForShutdown(3);
      System.err.println("------------------------");
      System.err.println("------------------------");
      System.err.println("------------------------");
      System.err.println("shutdown");
      return true;};
      return x;
  }
  private static void printStatus(){
    System.err.println("writePass: " + writePass.get());
    System.err.println("writeFail: " + writeFail.get());
    System.err.println("foundOnFirstRead: " + foundOnFirstRead.get());
    System.err.println("foundOnRetry: " + foundOnRetry.get());
  }
  
  private static void assertCorrectness(List<Future<Boolean>> d) throws InterruptedException, ExecutionException{
    List<Boolean> results = new ArrayList<>();
    for (Future<Boolean> t: d){
      results.add(t.get());
    }
    for (int i= 0;i< results.size();i++){
      Assert.assertEquals("Problem with key " + i,  true, results.get(i));
    }
    Assert.assertEquals(writePass.get(), foundOnFirstRead.get() + foundOnRetry.get());
  }
  
  public class WriteReadCallable implements Callable<Boolean> {

    private Session session;
    private int partitionKey;
    private PreparedStatement read;
    private PreparedStatement insert;
    
    public WriteReadCallable(Session session, int x, PreparedStatement ps, PreparedStatement i){
      this.session = session;
      this.partitionKey = x;
      this.read = ps;
      this.insert = i;
    }
    
    @Override
    public Boolean call() throws Exception {
      boolean pass = false;
      try { 
        insert(BatchStatement.Type.LOGGED, partitionKey + "", session, insert);
        System.err.println("Success write " + partitionKey);
        writePass.incrementAndGet();
      } catch (Exception ex){
        System.err.println("Failed write " + partitionKey);
        writeFail.incrementAndGet();
        return true;
      }
      List<Row> rows = null;
      int retry = 0;
      do {
        retry++;
        try {
          rows = session.execute(read.bind(partitionKey + "" + 2)).all();
        } catch (Exception e){
          System.err.println("Problem reading " + partitionKey + " " + e.getMessage());
        }
        if (rows != null && rows.size() == columnsPerPartition){
          System.err.println("found " + partitionKey + " on try " + retry);
          pass = true;
          if (retry == 1){
            foundOnFirstRead.incrementAndGet();
          } else {
            foundOnRetry.incrementAndGet();
          }
          break;
        }
        if (rows != null && rows.size() != columnsPerPartition) {
          System.err.println("found " + partitionKey + " with wrong data " + rows);
        }
        Thread.sleep(250);
      } while (retry < 100);
      return pass;
    }
    
  }
}
