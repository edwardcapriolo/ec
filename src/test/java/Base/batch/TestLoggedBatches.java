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
    PreparedStatement read = session.prepare("SELECT * FROM eventualtest.three where x=?");
    PreparedStatement ps = session.prepare("insert into eventualtest.three (x,y,z) values (?,?,?)");
    ExecutorService es = Executors.newFixedThreadPool(200);
    List<Callable<Boolean>> a = new ArrayList<>();
    for (int i =0;i<1000; i++){
      a.add(new WriteReadCallable(session, i, read, ps));
    }
    Callable<Boolean> x = () -> {
      fs1.get(1).getManager().destroyAndWaitForShutdown(3);
      System.err.println("------------------------");
      System.err.println("------------------------");
      System.err.println("------------------------");
      System.err.println("shutdown");
      return true;};
    a.set(350, x);
    List<Future<Boolean>> d = null;
    try {
      d = es.invokeAll(a);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    List<Boolean> results = new ArrayList<>();
    for (Future<Boolean> t: d){
      results.add(t.get());
    }
    System.err.println("writePass: " + writePass.get());
    System.err.println("writeFail: " + writeFail.get());
    System.err.println("foundOnFirstRead: " + foundOnFirstRead.get());
    System.err.println("foundOnRetry: " + foundOnRetry.get());
    for (int i= 0;i< results.size();i++){
      Assert.assertEquals("problem with key "+i,  true, results.get(i));
    }
    
  }  
  
  
  public class WriteReadCallable implements Callable<Boolean> {

    private Session session;
    private int x;
    private PreparedStatement ps;
    private PreparedStatement insert;
    
    public WriteReadCallable(Session session, int x, PreparedStatement ps, PreparedStatement i){
      this.session = session;
      this.x = x;
      this.ps = ps;
      this.insert = i;
    }
    
    @Override
    public Boolean call() throws Exception {
      boolean pass = false;
      try { 
        insert(BatchStatement.Type.LOGGED, x + "", session, insert);
        writePass.incrementAndGet();
      } catch (RuntimeException ex){
        writeFail.incrementAndGet();
        return true;
      }
      List<Row> rows = null;
      int retry = 0;
      do {
        retry++;
        try {
          rows = session.execute(ps.bind(x + "" + 2)).all();
        } catch (Exception e){
          
        }
        if (rows != null && rows.size() == columnsPerPartition){
          System.err.println("found on try " + retry);
          pass = true;
          if (retry == 1){
            foundOnFirstRead.incrementAndGet();
          } else{
            foundOnRetry.incrementAndGet();
          }
          break;
        }
      } while (retry < 100);
      
      return pass;
    }
    
  }
}
