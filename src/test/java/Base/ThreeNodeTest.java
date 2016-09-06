package Base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

public class ThreeNodeTest {

  private static List<Farsandra> fs1 = new ArrayList<>();

  @BeforeClass
  public static void setup() throws InterruptedException {
    fs1.add(build("127.0.0.1", "1"));
    fs1.add(build("127.0.0.2", "2"));
    fs1.add(build("127.0.0.3", "3"));
    
    fs1.add(build("127.0.0.11", "11"));
    fs1.add(build("127.0.0.12", "12"));
    fs1.add(build("127.0.0.13", "13"));
    
    fs1.add(build("127.0.0.21", "21"));
    fs1.add(build("127.0.0.22", "22"));
  }
  
  public static Farsandra build(String host, String instance) throws InterruptedException{
    Farsandra fs = new Farsandra();
    fs.withVersion("2.2.4");
    
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName(instance);
    fs.withCreateConfigurationFiles(true);
    fs.withHost(host);
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(10000 + Integer.parseInt(instance));
    fs.appendLinesToEnv("MAX_HEAP_SIZE=100M");
    fs.appendLinesToEnv("HEAP_NEWSIZE=10M");
    
    fs.withYamlReplacement("endpoint_snitch: SimpleSnitch",
            "endpoint_snitch:  GossipingPropertyFileSnitch");
    fs.appendLineToYaml("auto_bootstrap: false");
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
    for (Farsandra fs : ThreeNodeTest.fs1) {
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
    Cluster cluster;
    Session session;
    List<String> hosts = Arrays.asList("127.0.0.1");
    SocketOptions so = new SocketOptions().setReadTimeoutMillis(10000);
    cluster = Cluster.builder().addContactPoints(hosts.toArray(new String[] {}))
            .withSocketOptions(so).build();
    session = cluster.connect();
    System.out.println(cluster.getClusterName());
    Thread.sleep(500000);
  }

}