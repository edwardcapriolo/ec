package Base;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CompareAndSwapTest extends Base{

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101"));
  }
  
  @Test
  public void test() throws InterruptedException{
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE castest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE castest");
    session.execute("CREATE TABLE cas (x varchar, y bigint, primary key(x))");
    
    PreparedStatement insert = session.prepare("INSERT INTO castest.cas (x,y) VALUES ('3', ?)");
    
    Add a = new Add("127.0.0.101", 1000);
    Add b = new Add("127.0.0.101", 1000);
    Add c = new Add("127.0.0.101", 1000);
    
    session.execute(insert.bind(new Long(0)));
    ExecutorService es = Executors.newFixedThreadPool(10);
    List<Future<Void>> fut = es.invokeAll(Arrays.asList(a,b,c));
    waitForTheFuture(fut);

    PreparedStatement select = session.prepare("SELECT y from castest.cas WHERE x=?")
            .setConsistencyLevel(ConsistencyLevel.SERIAL);
    List<Row> rows = session.execute(select.bind("3")).all();
    Assert.assertEquals(1000 * 3 , rows.get(0).getLong("y"));        
    
  }
  
  public void waitForTheFuture(List<Future<Void>> fut) throws InterruptedException {
    for (Future<Void> f : fut){
      try {
        f.get();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
  }
}

class Add implements Callable<Void> {

  private final Session session;
  private final int numberOfInserts;
  
  public Add(String host, int numberOfInserts){
    session = Util.getSession(host);
    this.numberOfInserts = numberOfInserts;
  }
  
  @Override
  public Void call() {
    PreparedStatement select = session.prepare("SELECT y from castest.cas WHERE x=?")
            .setConsistencyLevel(ConsistencyLevel.ONE);
    PreparedStatement swap = session.prepare("UPDATE castest.cas SET y=? WHERE x='3' IF y=?")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    int passed = 0;
    do {
      try {
        List<Row> rows= session.execute(select.bind("3")).all();
        long found = rows.get(0).getLong("y");
        session.execute(swap.bind(found + 1, found));
        passed++;
      } catch (RuntimeException ex){
        ex.printStackTrace();
      }
    } while (passed < numberOfInserts);
    return null;
  }
  
}
