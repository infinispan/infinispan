package org.infinispan.scripting;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.impl.ScriptTask;
import org.infinispan.scripting.utils.ScriptingUtils;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptingTaskManagerTest")
@CleanupAfterMethod
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
      taskManager = extractGlobalComponent(cacheManager, TaskManager.class);
      cacheManager.defineConfiguration(ScriptingTest.CACHE_NAME, new ConfigurationBuilder().build());
   }

   public void testTask() throws Exception {
      ScriptingManager scriptingManager = extractGlobalComponent(cacheManager, ScriptingManager.class);
      ScriptingUtils.loadScript(scriptingManager, TEST_SCRIPT);
      String result = CompletionStages.join(taskManager.runTask(TEST_SCRIPT, new TaskContext().addParameter("a", "a")));
      assertEquals("a", result);

      List<Task> tasks = taskManager.getTasks();
      assertEquals(1, tasks.size());

      ScriptTask scriptTask = (ScriptTask) tasks.get(0);
      assertEquals("test.js", scriptTask.getName());
      assertEquals(TaskExecutionMode.ONE_NODE, scriptTask.getExecutionMode());
      assertEquals("Script", scriptTask.getType());
   }

   public void testAvailableEngines() {
      List<TaskEngine> engines = taskManager.getEngines();
      assertEquals(1, engines.size());
      assertEquals("Script", engines.get(0).getName());
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Script execution error.*")
   public void testBrokenTask() throws Throwable {
      ScriptingManager scriptingManager = extractGlobalComponent(cacheManager, ScriptingManager.class);
      ScriptingUtils.loadScript(scriptingManager, BROKEN_SCRIPT);
      try {
         CompletionStages.join(taskManager.runTask(BROKEN_SCRIPT, new TaskContext()));
      } catch (CompletionException e) {
         throw CompletableFutures.extractException(e);
      }
   }
}
