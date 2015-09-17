package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptingTaskManagerTest")
public class ScriptingTaskManagerTest extends SingleCacheManagerTest {

   protected static final String SCRIPT_NAME = "test.js";
   protected TaskManager taskManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
      Cache<String, String> scriptCache = cacheManager.getCache(ScriptingManager.SCRIPT_CACHE);
      try (InputStream is = this.getClass().getResourceAsStream("/test.js")) {
         String script = TestingUtil.loadFileAsString(is);
         scriptCache.put(SCRIPT_NAME, script);
      }
   }

   public void testTask() throws Exception {
      String result = (String) taskManager.runTask(SCRIPT_NAME, new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }
}
