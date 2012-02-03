package org.infinispan.loaders.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;

public class EmbeddedServerHelper {
   public static int zooKeeperPort = -1;
   private static String zkBaseDir = System.getProperty("java.io.tmpdir") + "/embedded-zk";
   private static String hbaseRootDir = System.getProperty("java.io.tmpdir") + "/embedded-hbase-"
            + System.currentTimeMillis();

   public EmbeddedServerHelper() {
   }

   static ExecutorService executor = Executors.newSingleThreadExecutor();

   /**
    * Set embedded hbase up and spawn it in a new thread.
    * 
    * @throws InterruptedException
    */
   public void setup() throws InterruptedException {
      executor.execute(new HBaseRunner());
      try {
         TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   public static void teardown() throws IOException {
      executor.shutdown();
      executor.shutdownNow();

      // Delete the temp data dirs
      FileUtils.deleteDirectory(new File(zkBaseDir));
      FileUtils.deleteDirectory(new File(hbaseRootDir));
   }

   class HBaseRunner implements Runnable {
      @Override
      public void run() {
         Configuration conf = HBaseConfiguration.create();

         try {
            MiniZooKeeperCluster zkCluster = new MiniZooKeeperCluster(conf);
            EmbeddedServerHelper.zooKeeperPort = zkCluster.startup(new File(zkBaseDir + "/"
                     + String.valueOf(System.currentTimeMillis())));

            // Overwrite the ZooKeeper client port with the embedded port
            conf.set("hbase.zookeeper.property.clientPort",
                     Integer.toString(EmbeddedServerHelper.zooKeeperPort));

            // Overwrite the hbase root dir with a unique, temporary dir
            conf.set("hbase.rootdir", hbaseRootDir);

            LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1, 1);
            cluster.startup();
         } catch (Exception ex) {
            System.err.println("Exception happened when running HBase: " + ex.getMessage());
         }
      }
   }
}
