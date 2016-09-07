package Base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

public class ThreeDcTest {

  private static List<Farsandra> fs1 = new ArrayList<>();

  @BeforeClass
  public static void setup() throws InterruptedException {
    fs1.add(build("127.0.0.11", "11"));
    fs1.add(build("127.0.0.12", "12"));
    fs1.add(build("127.0.0.13", "13"));
    
    fs1.add(build("127.0.0.21", "21"));
    fs1.add(build("127.0.0.22", "22"));
    fs1.add(build("127.0.0.33", "23"));
    
    fs1.add(build("127.0.0.31", "31"));
    fs1.add(build("127.0.0.32", "32"));
  }
  
  public static Farsandra build(String host, String instance) throws InterruptedException{
    Farsandra fs = new Farsandra();
    fs.withVersion("2.2.4");
    
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName(instance);
    fs.withCreateConfigurationFiles(true);
    fs.withHost(host);
    fs.withSeeds(Arrays.asList("127.0.0.11"));
    fs.withJmxPort(10000 + Integer.parseInt(instance));
    fs.appendLinesToEnv("MAX_HEAP_SIZE=100M");
    fs.appendLinesToEnv("HEAP_NEWSIZE=10M");
    
    fs.withYamlReplacement("endpoint_snitch: SimpleSnitch",
            "endpoint_snitch:  GossipingPropertyFileSnitch");
    fs.appendLineToYaml("auto_bootstrap: false");
    fs.withDatacentername(instance.charAt(0)+"");
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler(new LineHandler() {
      @Override
      public void handleLine(String line) {
        System.out.println("out " + line);
        if (line.contains("Listening for thrift clients...")) {
          started.countDown();
        }
      }
    });
    fs.getManager().addProcessHandler(new ProcessHandler() {
      @Override
      public void handleTermination(int exitValue) {
        System.out.println("Cassandra terminated with exit value: " + exitValue);
        started.countDown();
      }
    });
    fs.start();
    started.await(10, TimeUnit.SECONDS);
    return fs;
  }
  

  @AfterClass
  public static void close() {
    for (Farsandra fs : ThreeDcTest.fs1) {
      if (fs != null) {
        try {
          fs.getManager().destroyAndWaitForShutdown(6);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void hello() throws InterruptedException {
    
    List<String> hosts = Arrays.asList("127.0.0.11");
    SocketOptions so = new SocketOptions().setReadTimeoutMillis(10000);
    Cluster cluster = Cluster.builder().addContactPoints(hosts.toArray(new String[] {}))
            .withSocketOptions(so).build();
    Session session = cluster.connect();
    
    session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', '1' : 3, '2' : 3, '3': 1 }");
    session.execute("USE test");
    session.execute("CREATE TABLE kv (x varchar, y varchar, primary key(x))");
    
    PreparedStatement oneWrite = session.prepare("INSERT INTO test.kv (x , y) VALUES (?, ?)")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    PreparedStatement localquorumWrite = session.prepare("INSERT INTO test.kv (x , y)  VALUES (?, ?)")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    PreparedStatement eachquorumWrite = session.prepare("INSERT INTO test.kv (x , y)   VALUES (?, ?)")
            .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
    PreparedStatement quorumWrite = session.prepare("INSERT INTO test.kv (x , y)     VALUES (?, ?)")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
      
    session.execute(oneWrite.bind("mykey", "val1"));
    session.execute(localquorumWrite.bind("mykey", "val2"));
    session.execute(eachquorumWrite.bind("mykey", "val3"));
    session.execute(quorumWrite.bind("mykey", "val4"));
  
    
    fs1.get(5).getManager().destroyAndWaitForShutdown(6);
    session.execute(oneWrite.bind("mykey", "val5"));
    session.execute(localquorumWrite.bind("mykey", "val6"));
    session.execute(eachquorumWrite.bind("mykey", "val7"));
    session.execute(quorumWrite.bind("mykey", "val8"));

    
    fs1.get(7).getManager().destroyAndWaitForShutdown(6);
    session.execute(oneWrite.bind("mykey", "val4"));
    session.execute(localquorumWrite.bind("mykey", "val5"));
    session.execute(quorumWrite.bind("mykey", "val8"));
    // assertException(session, eachquorumWrite); //possible bug here
    
    
    fs1.get(6).getManager().destroyAndWaitForShutdown(6);
    fs1.get(3).getManager().destroyAndWaitForShutdown(6);
    fs1.get(4).getManager().destroyAndWaitForShutdown(6);
    
    session.execute(oneWrite.bind("mykey", "val4"));
    session.execute(localquorumWrite.bind("mykey", "val5"));
    assertException(session, eachquorumWrite);
    assertException(session, quorumWrite); //possible bug here
    
  }
  
  public void assertException(Session session, PreparedStatement ps){
    try {
      session.execute(ps.bind("mykey", "val6"));
      Assert.fail("This should not have worked");
    } catch (Exception e){
      System.out.println("this did not work we expected that");
    }
  }

}
