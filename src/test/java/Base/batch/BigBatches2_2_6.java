package Base.batch;



import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import Base.Base;
import Base.Util;

public class BigBatches2_2_6 extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.101", "101", "127.0.0.101", "2.2.6"));
    fs1.add(Util.build("127.0.0.102", "102", "127.0.0.101", "2.2.6"));
    fs1.add(Util.build("127.0.0.103", "103", "127.0.0.101", "2.2.6"));
  }
  
  
  @Test(expected=InvalidQueryException.class)
  //Batch too large ~1700
  public void aTest(){
    BigBatches3_7.keepBatchingTillYouDie(BatchStatement.Type.LOGGED);
  }  
}
