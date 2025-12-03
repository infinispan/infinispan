package org.infinispan.server.resp.persistent;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.test.TestSetup;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "server.resp.persistent.DistributedPersistentCommandTest")
public class ClusteredPersistentStringCommandTest extends PersistentStringCommandTest {

   private CacheMode mode;

   @Override
   public Object[] factory() {
      return new Object[]{
         new ClusteredPersistentStringCommandTest().withCacheMode(CacheMode.DIST_SYNC),
         new ClusteredPersistentStringCommandTest().withCacheMode(CacheMode.REPL_SYNC),
         new ClusteredPersistentStringCommandTest().withCacheMode(CacheMode.DIST_SYNC).withAuthorization(),
         new ClusteredPersistentStringCommandTest().withCacheMode(CacheMode.REPL_SYNC).withAuthorization()
      };
   }

   protected ClusteredPersistentStringCommandTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void removeData() {
      Util.recursiveFileRemove(baseFolderName());
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() throws Throwable {
      Util.recursiveFileRemove(baseFolderName());
      destroy();
      super.createBeforeMethod();
   }

   @Override
   protected String parameters() {
      return "[mode=" + mode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.clustering().cacheMode(mode);
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(3);
   }

   @Override
   protected String nodeId() {
      return super.nodeId() + cacheManagers.size();
   }
}
