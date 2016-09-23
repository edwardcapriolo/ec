package Base.batch;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import Base.Base;
import Base.Util;
import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

public class BigBatches2_2_6_tweeked extends Base {

  @Before
  public void setup() throws InterruptedException {
    fs1.add(buildTweeked("127.0.0.101", "101", "127.0.0.101", "2.2.6"));
    fs1.add(buildTweeked("127.0.0.102", "102", "127.0.0.101", "2.2.6"));
    fs1.add(buildTweeked("127.0.0.103", "103", "127.0.0.101", "2.2.6"));
  }
  
  @Test(expected=IllegalStateException.class)
  //java.lang.IllegalStateException: Batch statement cannot contain more than 65535 statements.
  public void aTest(){
    BigBatches3_7.keepBatchingTillYouDie();
  }  
  
  
  public static Farsandra buildTweeked(String host, String instance, String seed, String version) throws InterruptedException{
    Farsandra fs = new Farsandra();
    fs.withVersion(version);
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName(instance);
    fs.withCreateConfigurationFiles(true);
    fs.withHost(host);
    fs.withSeeds(Arrays.asList(seed));
    fs.withJmxPort(10000 + Integer.parseInt(instance));
    fs.appendLinesToEnv("MAX_HEAP_SIZE=100M");
    fs.appendLinesToEnv("HEAP_NEWSIZE=10M");
    fs.withYamlReplacement("endpoint_snitch: SimpleSnitch",
            "endpoint_snitch:  GossipingPropertyFileSnitch");
    fs.withYamlReplacement("batch_size_warn_threshold_in_kb: 5", 
            "batch_size_warn_threshold_in_kb: 728");
    fs.withYamlReplacement("batch_size_fail_threshold_in_kb: 50", 
            "batch_size_fail_threshold_in_kb: 5000");
    fs.withYamlReplacement("commitlog_segment_size_in_mb: 32", 
            "commitlog_segment_size_in_mb: 64");
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
  
}
