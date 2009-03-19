package org.horizon.profiling;

import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.ProfileTestSlave")
public class ProfileTestSlave extends AbstractProfileTest {
   @Test(enabled = true)
   public void testReplMode() throws Exception {
      cache = cacheManager.getCache(REPL_SYNC_CACHE_NAME);
      System.out.println("Waiting for test completion.  Hit any key when done.");
      System.in.read();
   }
}
