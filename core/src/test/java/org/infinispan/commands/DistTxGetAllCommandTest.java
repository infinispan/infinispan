package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author William Burns
 */
@Test(groups = "functional")
public abstract class DistTxGetAllCommandTest extends GetAllCommandTest {

   protected DistTxGetAllCommandTest(CacheMode cacheMode, boolean transactional) {
      super(cacheMode, transactional);
   }

   @Test(groups = "functional", testName="org.infinispan.commands.DistGetAllCommandTest$DistTx")
   public static class DistTx extends DistTxGetAllCommandTest {
      protected DistTx() {
         super(CacheMode.DIST_SYNC, true);
      }
   }

   @Test(groups = "functional", testName="org.infinispan.commands.DistGetAllCommandTest$DistTxCompatibility")
   public static class DistTxCompatibility extends DistTxGetAllCommandTest {
      protected DistTxCompatibility() {
         super(CacheMode.DIST_SYNC, true);
      }

      @Override
      protected void amendConfiguration(ConfigurationBuilder builder) {
         super.amendConfiguration(builder);
         enableCompatibility(builder);
      }
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      builder.transaction().locking().isolationLevel(IsolationLevel.READ_COMMITTED);
   }
}
