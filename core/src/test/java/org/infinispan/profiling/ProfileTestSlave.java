package org.infinispan.profiling;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
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
      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               System.in.read();
            } catch (Exception e) {
            }
         }
      };

      // attach a view change listener
      cacheManager.addListener(new ShutdownHook(t));
      System.out.println("Slave listening for remote connections.  Hit Enter when done.");

      t.start();
      t.join();      
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

   @Listener
   public static final class ShutdownHook {
      final Thread completionThread;

      public ShutdownHook(Thread completionThread) {
         this.completionThread = completionThread;
      }

      @ViewChanged
      public void viewChanged(ViewChangedEvent vce) {
         System.out.println("Saw view change event " + vce);
         // if the new view ONLY contains me, die!
         if (vce.getOldMembers().size() > vce.getNewMembers().size() && vce.getNewMembers().size() == 1 && vce.getNewMembers().contains(vce.getLocalAddress())) {
            completionThread.interrupt();
         }
      }
   }
}
