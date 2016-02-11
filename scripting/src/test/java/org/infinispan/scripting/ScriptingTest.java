package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.ScriptingTest")
public class ScriptingTest extends SingleCacheManagerTest {

   protected ScriptingManager scriptingManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   protected String[] getScripts() {
      return new String[] { "test.js" };
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
      for (String scriptName : getScripts()) {
         loadScript(scriptingManager, scriptName);
      }
   }

   public static void loadScript(ScriptingManager scriptingManager, String scriptName) throws IOException {
      try (InputStream is = ScriptingTest.class.getResourceAsStream("/" + scriptName)) {
         String script = TestingUtil.loadFileAsString(is);
         scriptingManager.addScript(scriptName, script);
      }
   }

   public void testSimpleScript() throws Exception {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }
}
