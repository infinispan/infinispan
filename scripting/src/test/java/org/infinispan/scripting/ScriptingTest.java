package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.io.InputStream;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptingTest")
public class ScriptingTest extends SingleCacheManagerTest {

   protected static final String SCRIPT_NAME = "test.js";
   protected ScriptingManager scriptingManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
      try (InputStream is = this.getClass().getResourceAsStream("/test.js")) {
         String script = TestingUtil.loadFileAsString(is);
         scriptingManager.addScript(SCRIPT_NAME, script);
      }
   }

   public void testSimpleScript() throws Exception {
      String result = (String) scriptingManager.runScript(SCRIPT_NAME).get();
      assertEquals("a", result);
   }
}
