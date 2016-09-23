package Base.batch;



import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import Base.Base;
import Base.Util;

public class BigBatches3_7 extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101", "3.7"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101", "3.7"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101", "3.7"));
  }
  
  public static void keepBatchingTillYouDie(){
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE eventualtest");
    session.execute("CREATE TABLE three (x varchar, y varchar, z varchar, primary key(x,y))");
    int insertsInBatch = 1000;
    int endInsertsInBatch = 1000000;
    PreparedStatement ps = session.prepare("insert into three (x,y,z) values (?,?,?)");
    System.out.println("batch_size\tbatch_time");
    for (int i = insertsInBatch; i < endInsertsInBatch ; i= (int) (i * 1.2D)){
      BatchStatement bs = new BatchStatement();
      long start = System.currentTimeMillis();
      for (int j = 0 ; j < i; j++){
        bs.add(ps.bind("1", j + "", j + "3"));  
      }
      session.execute(bs);
      System.out.println(i+"\t"+ (System.currentTimeMillis() - start));
    }
  }
  
  @Test(expected=InvalidQueryException.class)
  //last success is size 1728
  public void aTest(){
    keepBatchingTillYouDie();
  }  
}
