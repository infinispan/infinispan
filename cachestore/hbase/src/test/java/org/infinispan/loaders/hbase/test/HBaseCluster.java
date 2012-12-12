package org.infinispan.loaders.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages lifecycle of an HBase cluster
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class HBaseCluster {

   private static final Log log = LogFactory.getLog(HBaseCluster.class);

   private static final ThreadLocal<Integer> masterPort = new ThreadLocal<Integer>() {
      private final AtomicInteger port = new AtomicInteger(61010);

      @Override
      protected Integer initialValue() {
         return port.getAndAdd(100);
      }
   };

   private final HBaseTestingUtility testUtil;
   private final int zooKeeperPort;

   public HBaseCluster() {
      log.info("Starting HBase cluster");
      Configuration conf = HBaseConfiguration.create();
      conf.setInt("hbase.master.assignment.timeoutmonitor.period", 2000);
      conf.setInt("hbase.master.assignment.timeoutmonitor.timeout", 5000);
      conf.setInt("hbase.master.info.port", masterPort.get());

      testUtil = new HBaseTestingUtility(conf);
      try {
         testUtil.startMiniCluster();
         MiniHBaseCluster cluster = testUtil.getHBaseCluster();
         log.info("Waiting for active/ready HBase master");
         cluster.waitForActiveAndReadyMaster();
         // Cache zoo keeper port for cache store configuration
         zooKeeperPort = testUtil.getConfiguration()
               .getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1);
      } catch (Exception e) {
         throw new RuntimeException("Unable to start HBase cluster", e);
      }
   }

   public int getZooKeeperPort() {
      return zooKeeperPort;
   }

   private void shutdown() {
      try {
         testUtil.shutdownMiniCluster();
      } catch (Exception e) {
         log.warn("Problems shutting down HBase cluster", e);
      }
   }

   public static void shutdown(HBaseCluster hbaseCluster) {
      if (hbaseCluster != null)
         hbaseCluster.shutdown();
   }

}
