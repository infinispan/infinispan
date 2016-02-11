package org.infinispan.scripting;

import static org.infinispan.scripting.ScriptingTests.loadScript;
import static org.testng.AssertJUnit.assertEquals;

import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptingTaskManagerTest")
public class ScriptingTaskManagerTest extends SingleCacheManagerTest {

   protected static final String TEST_SCRIPT = "test.js";
   protected static final String BROKEN_SCRIPT = "brokenTest.js";
   protected TaskManager taskManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
   }

   public void testTask() throws Exception {
      ScriptingManager scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
      loadScript(scriptingManager, TEST_SCRIPT);
      String result = (String) taskManager.runTask(TEST_SCRIPT, new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN026003.*")
   public void testBrokenTask() throws Exception {
      ScriptingManager scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
      loadScript(scriptingManager, BROKEN_SCRIPT);
      taskManager.runTask(BROKEN_SCRIPT, new TaskContext()).get();
   }
}
