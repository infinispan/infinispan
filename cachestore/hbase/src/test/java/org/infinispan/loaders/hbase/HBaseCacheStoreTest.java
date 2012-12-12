/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.hbase.test.HBaseCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.hbase.HBaseCacheStoreTest")
public class HBaseCacheStoreTest extends BaseCacheStoreTest {

   HBaseCluster hBaseCluster;

   //   private static final boolean USE_EMBEDDED = true;
//
//   private EmbeddedServerHelper embedded;

//   HBaseTestingUtility testUtil;
//   MiniHBaseCluster cluster;
//   private int zooKeeperPort;

   @BeforeClass(alwaysRun = true)
   public void beforeClass() throws Exception {
      hBaseCluster = new HBaseCluster();

//      log.info("Starting HBase cluster");
//      Configuration conf = HBaseConfiguration.create();
//      conf.setInt("hbase.master.assignment.timeoutmonitor.period", 2000);
//      conf.setInt("hbase.master.assignment.timeoutmonitor.timeout", 5000);
//      testUtil = new HBaseTestingUtility(conf);
//      testUtil.startMiniCluster();
//      cluster = testUtil.getHBaseCluster();
//      log.info("Waiting for active/ready HBase master");
//      cluster.waitForActiveAndReadyMaster();
//
//      zooKeeperPort = testUtil.getConfiguration()
//            .getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1);

//      if (USE_EMBEDDED) {
//         embedded = new EmbeddedServerHelper();
//         embedded.setup();
//      }

//      super.setUp();
   }

   @AfterClass(alwaysRun = true)
   public void afterClass() throws CacheLoaderException {
      HBaseCluster.shutdown(hBaseCluster);

//      try {
//         testUtil.shutdownMiniCluster();
//      } catch (Exception e) {
//         throw new CacheLoaderException(e);
//      }
   }

   @Override
   protected CacheStore createCacheStore() throws Exception {
      HBaseCacheStore cs = new HBaseCacheStore();
      // This uses the default config settings in HBaseCacheStoreConfig
      HBaseCacheStoreConfig conf = new HBaseCacheStoreConfig();
      conf.setPurgeSynchronously(true);

      // overwrite the ZooKeeper client port with the port from the embedded server
      conf.setHbaseZookeeperPropertyClientPort(hBaseCluster.getZooKeeperPort());

      cs.init(conf, getCache(), getMarshaller());
      cs.start();
      return cs;
   }

}