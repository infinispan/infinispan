package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author William Burns
 */
@Test(groups = "functional")
public abstract class DistGetAllCommandTest extends GetAllCommandTest {

   protected DistGetAllCommandTest(CacheMode cacheMode, boolean transactional) {
      super(cacheMode, transactional);
   }

   @Test(groups = "functional", testName="org.infinispan.commands.DistGetAllCommandTest$DistNonTx")
   public static class DistNonTx extends DistGetAllCommandTest {
      protected DistNonTx() {
         super(CacheMode.DIST_SYNC, false);
      }
   }

   @Test(groups = "functional", testName="org.infinispan.commands.DistGetAllCommandTest$DistNonTxCompatibility")
   public static class DistNonTxCompatibility extends DistGetAllCommandTest {
      protected DistNonTxCompatibility() {
         super(CacheMode.DIST_SYNC, false);
      }

      @Override
      protected void amendConfiguration(ConfigurationBuilder builder) {
         enableCompatibility(builder);
      }
   }
}
