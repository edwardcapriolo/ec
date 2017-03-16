package Base;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

public class Util {

  public static Farsandra build(String host, String instance, String seed) throws InterruptedException {
    return build(host, instance, seed, "2.2.6");
  }
  
  public static Farsandra build(String host, String instance, String seed, String version) throws InterruptedException{
    Farsandra fs = new Farsandra();
    fs.withVersion(version);
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName(instance);
    fs.withCreateConfigurationFiles(true);
    fs.withHost(host);
    fs.withSeeds(Arrays.asList(seed));
    fs.withJmxPort(10000 + Integer.parseInt(instance));
    fs.withDatacentername(instance.charAt(0)+"");
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
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
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
  
  public static Session getSession(String host){
    List<String> hosts = Arrays.asList(host);
    SocketOptions so = new SocketOptions().setReadTimeoutMillis(10000);
    Cluster cluster = Cluster.builder().addContactPoints(hosts.toArray(new String[] {})).
            withQueryOptions(new QueryOptions().setFetchSize(2000))
            .withSocketOptions(so).build();
    
    Session session = cluster.connect();
    return session;
  }
}
