package org.infinispan.profiling;

import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.ProfileTestSlave")
public class ProfileTestSlave extends AbstractProfileTest {

   public static void main(String[] args) throws Exception {
      ProfileTestSlave pst = new ProfileTestSlave();
      pst.startedInCmdLine = true;

      String mode = args[0];
      try {
         if (args.length > 1) pst.clusterNameOverride = args[1];
         pst.testWith(mode);
      } finally {
         pst.destroyAfterMethod();
         pst.destroyAfterClass();
      }
   }

   public void testReplSync() throws Exception {
      testWith(REPL_SYNC_CACHE_NAME);
   }

   public void testReplAsync() throws Exception {
      testWith(REPL_ASYNC_CACHE_NAME);
   }

   public void testDistSync() throws Exception {
      testWith(DIST_SYNC_CACHE_NAME);
   }

   public void testDistAsync() throws Exception {
      testWith(DIST_ASYNC_CACHE_NAME);
   }

   public void testDistSyncL1() throws Exception {
      testWith(DIST_SYNC_L1_CACHE_NAME);
   }

   public void testDistAsyncL1() throws Exception {
      testWith(DIST_ASYNC_L1_CACHE_NAME);
   }

   private void waitForTest() throws Exception {
      System.out.println("Slave listening for remote connections.  Hit Enter when done.");
      System.in.read();
   }

   private void doTest() {
      // trigger for JProfiler
   }

   protected void testWith(String cachename) throws Exception {
      log.warn("Starting slave, cache name = {0}", cachename);
      initTest();
      cache = cacheManager.getCache(cachename);
      System.out.println("Waiting for test completion.  Hit any key when done.");
      doTest();
      waitForTest();
   }
}
