package Base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import io.teknek.farsandra.Farsandra;

/*
 * CREATE KEYSPACE sample WITH replication = {'class': 'NetworkTopologyStrategy', 'gce-backup': '1', 'office': '1', 'prod': '3', 'prod2': '3'}  AND durable_writes = true;

CREATE TABLE sample.sample (
    id uuid PRIMARY KEY,
    created_at timestamp,
    data text,
    date text,
    deleted boolean,
    status tinyint,
    updated_at timestamp,
    user_id text,
    vendor text
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW sample.user_items AS
    SELECT user_id, id, created_at
    FROM sample.sample
    WHERE id IS NOT NULL AND user_id IS NOT NULL AND created_at IS NOT NULL
    PRIMARY KEY (user_id, id)
    WITH CLUSTERING ORDER BY (id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW sample.items_by_date AS
    SELECT date, id
    FROM sample.sample
    WHERE id IS NOT NULL AND date IS NOT NULL
    PRIMARY KEY (date, id)
    WITH CLUSTERING ORDER BY (id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
 */
public class QuorumMvTest {

  private static List<Farsandra> fs1 = new ArrayList<>();

  @BeforeClass
  public static void setup() throws InterruptedException {
    fs1.add(Util.build("127.0.0.51", "51", "127.0.0.51", "3.9"));
    fs1.add(Util.build("127.0.0.52", "52", "127.0.0.51", "3.9"));
    fs1.add(Util.build("127.0.0.53", "53", "127.0.0.51", "3.9"));
    
    fs1.add(Util.build("127.0.0.61", "61", "127.0.0.51", "3.9"));
    fs1.add(Util.build("127.0.0.62", "62", "127.0.0.51", "3.9"));
    fs1.add(Util.build("127.0.0.63", "63", "127.0.0.51", "3.9"));
    
    fs1.add(Util.build("127.0.0.71", "71", "127.0.0.51", "3.9"));
    fs1.add(Util.build("127.0.0.72", "72", "127.0.0.51", "3.9"));
  }
  
  
  
  @AfterClass
  public static void close() {
    for (Farsandra fs : fs1) {
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
    
    List<String> hosts = Arrays.asList("127.0.0.51");
    SocketOptions so = new SocketOptions().setReadTimeoutMillis(10000);
    Cluster cluster = Cluster.builder().addContactPoints(hosts.toArray(new String[] {}))
            .withSocketOptions(so).build();
    Session session = cluster.connect();
    
    
    session.execute("CREATE KEYSPACE sample WITH replication = "
            + "{'class': 'NetworkTopologyStrategy', '5': '3', '6': '3', '7': '2'}  AND durable_writes = true");
    
    session.execute(
            "CREATE TABLE sample.sample ( "+
            "        id uuid PRIMARY KEY, "+
            "        created_at timestamp, "+
            "        data text, "+
            "        date text, "+
            "        deleted boolean, "+
            "        status tinyint,  "+
            "        updated_at timestamp,  "+
            "        user_id text,  "+
            "        vendor text  "+
            "    )  "
            );
    
    session.execute(
            "CREATE TABLE sample.nomv ( "+
            "        id uuid PRIMARY KEY, "+
            "        created_at timestamp, "+
            "        data text, "+
            "        date text, "+
            "        deleted boolean, "+
            "        status tinyint,  "+
            "        updated_at timestamp,  "+
            "        user_id text,  "+
            "        vendor text  "+
            "    )  "
            );
    
    session.execute("CREATE MATERIALIZED VIEW sample.user_items AS "+
    "SELECT user_id, id, created_at "+
    "FROM sample.sample "+
    "WHERE id IS NOT NULL AND user_id IS NOT NULL AND created_at IS NOT NULL "+
    "PRIMARY KEY (user_id, id) "+
    "WITH CLUSTERING ORDER BY (id ASC) "+
    "");
    
    
    fs1.get(3).getManager().destroyAndWaitForShutdown(6);
    fs1.get(4).getManager().destroyAndWaitForShutdown(6);
    fs1.get(5).getManager().destroyAndWaitForShutdown(6);
    fs1.get(6).getManager().destroyAndWaitForShutdown(6);
    fs1.get(7).getManager().destroyAndWaitForShutdown(6);
    
    
    PreparedStatement localquorumWrite1 = session.prepare("INSERT INTO sample.nomv (id)  VALUES (uuid())")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    session.execute(localquorumWrite1.bind());
    
    PreparedStatement localquorumWrite2 = session.prepare("INSERT INTO sample.sample (id)  VALUES (uuid())")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    for (int i = 0 ; i <10000; i++){
      session.execute(localquorumWrite2.bind());
    }
  }
}
