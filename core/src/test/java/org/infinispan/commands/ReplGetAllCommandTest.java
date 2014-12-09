package org.infinispan.commands;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author William Burns
 */
@Test(groups = "functional")
public abstract class ReplGetAllCommandTest extends GetAllCommandTest {

   protected ReplGetAllCommandTest(CacheMode cacheMode, boolean transactional) {
      super(cacheMode, transactional);
   }

   @Test(groups = "functional", testName="org.infinispan.commands.ReplGetAllCommandTest$ReplNonTx")
   public static class ReplNonTx extends ReplGetAllCommandTest {
      protected ReplNonTx() {
         super(CacheMode.REPL_SYNC, false);
      }
   }

   @Test(groups = "functional", testName="org.infinispan.commands.ReplGetAllCommandTest$ReplNonTxCompatibility")
   public static class ReplNonTxCompatibility extends ReplGetAllCommandTest {
      protected ReplNonTxCompatibility() {
         super(CacheMode.REPL_SYNC, false);
      }

      @Override
      protected void amendConfiguration(ConfigurationBuilder builder) {
         enableCompatibility(builder);
      }
   }

   @Test(groups = "functional", testName="org.infinispan.commands.ReplGetAllCommandTest$ReplTx")
   public static class ReplTx extends ReplGetAllCommandTest {
      protected ReplTx() {
         super(CacheMode.REPL_SYNC, true);
      }
   }

   @Test(groups = "functional", testName="org.infinispan.commands.ReplGetAllCommandTest$ReplTxCompatibility")
   public static class ReplTxCompatibility extends ReplGetAllCommandTest {
      protected ReplTxCompatibility() {
         super(CacheMode.REPL_SYNC, true);
      }

      @Override
      protected void amendConfiguration(ConfigurationBuilder builder) {
         enableCompatibility(builder);
      }
   }
}
