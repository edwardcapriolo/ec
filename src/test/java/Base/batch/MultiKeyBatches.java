package Base.batch;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import Base.Base;
import Base.Util;


public class MultiKeyBatches extends Base {
  
  public static int columnsPerPartition = 2000;
  
  @Before
  public void setup() throws InterruptedException {
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.101", "101", "127.0.0.101", "3.7"));
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.102", "102", "127.0.0.101", "3.7"));
    fs1.add(BigBatches2_2_6_tweeked.buildTweeked("127.0.0.103", "103", "127.0.0.101", "3.7"));
  }

  public static void insert(BatchStatement.Type statementType, String keybase, Session session){
    session.execute("USE eventualtest");
    int numberOfPartitions = 3;
    PreparedStatement ps = session.prepare("insert into three (x,y,z) values (?,?,?)");
    BatchStatement bs = new BatchStatement(statementType);
    for (int i = 0; i < numberOfPartitions ; i++){
      for (int j = 0 ; j < columnsPerPartition; j++){
        bs.add(ps.bind(keybase + i +"", j + "", j + "3"));  
      }
    }
    session.execute(bs);
  }
  
  @Test
  /*
   * With 3 rowkeys in a batch, even logged we still have "eventual consistency". 
   * We always see ALL the rows in a partition.
   * Each partition  will appear eventually:
   * For a given run results will look like this:
   * all: 24
   * none: 6
   * some: 0 <-- Should always be 0
   */
  public void aTest(){
    Session session = Util.getSession("127.0.0.101");
    session.execute("CREATE KEYSPACE eventualtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("CREATE TABLE eventualtest.three (x varchar, y varchar, z varchar, primary key(x,y))");
    PreparedStatement ps = session.prepare("SELECT * FROM eventualtest.three where x=?");
    long allFound = 0;
    long noneFound = 0;
    long someFound = 0;
    int testAttempts = 30;
    for (int i = 0; i < testAttempts; i++) {
      insert(BatchStatement.Type.LOGGED, i + "", session);
      List<Row> rows = session.execute(ps.bind(i+""+2)).all();
      if (rows.size() == MultiKeyBatches.columnsPerPartition){
        allFound++;
      } else if (rows.size() == 0){
        noneFound++;
      } else {
        System.out.println(rows);
        someFound++;
      }
    }
    System.err.println("all: " + allFound);
    System.err.println("none: " + noneFound);
    System.err.println("some: " + someFound);
    Assert.assertEquals(testAttempts, allFound + noneFound);
    Assert.assertEquals(0, someFound);
  }  
  
}