package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import static org.infinispan.scripting.ScriptingTests.loadScript;

@Test(groups = "functional", testName = "scripting.ScriptingTest")
public class ScriptingTest extends SingleCacheManagerTest {

   protected ScriptingManager scriptingManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
   }



   public void testSimpleScript() throws Exception {
      loadScript(scriptingManager, "test.js");
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN026005:.*")
   public void testScriptRemove() throws Exception {
      loadScript(scriptingManager, "test.js");
      scriptingManager.getScript("test.js");
      scriptingManager.removeScript("test.js");
      scriptingManager.getScript("test.js");
   }
}
