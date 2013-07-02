package org.infinispan.loaders.hbase;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.hbase.test.HBaseCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.hbase.HBaseCacheStoreTest")
public class HBaseCacheStoreTest extends BaseCacheStoreTest {

   HBaseCluster hBaseCluster;

   @BeforeClass
   public void beforeClass() throws Exception {
      hBaseCluster = new HBaseCluster();
   }

   @AfterClass
   public void afterClass() throws CacheLoaderException {
      HBaseCluster.shutdown(hBaseCluster);
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
