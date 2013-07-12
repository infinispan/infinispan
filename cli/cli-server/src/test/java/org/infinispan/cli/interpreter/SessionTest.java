package org.infinispan.cli.interpreter;

import java.util.Map;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli-server.SessionTest")
public class SessionTest extends SingleCacheManagerTest {

   public void testSessionExpiration() throws Exception {
      Interpreter interpreter = new Interpreter();
      interpreter.initialize(this.cacheManager, TIME_SERVICE);
      interpreter.setSessionTimeout(500);
      interpreter.setSessionReaperWakeupInterval(1000);
      interpreter.start();

      try {
         String sessionId = interpreter.createSessionId(null);
         Thread.sleep(1500);
         Map<String, String> response = interpreter.execute(sessionId, "");
         assert response.containsKey("ERROR");
      } finally {
         interpreter.stop();
      }

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }
}
