package org.infinispan.scripting;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.BlockHoundHelper;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.impl.ScriptTask;
import org.infinispan.scripting.impl.ScriptingManagerImpl;
import org.infinispan.scripting.utils.ScriptingUtils;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.manager.TaskManager;
import org.infinispan.tasks.manager.spi.TaskEngine;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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

   public void testContainsScriptAsyncWhenUninitialized() throws Exception {
      ScriptingManager scriptingManager = extractGlobalComponent(cacheManager, ScriptingManager.class);
      ScriptingManagerImpl scriptingManagerImpl = (ScriptingManagerImpl) scriptingManager;

      // Simulate scriptCache not being initialized yet
      TestingUtil.replaceField(null, "scriptCache", scriptingManagerImpl, ScriptingManagerImpl.class);

      // Ensure containsScriptAsync can be called from a non-blocking context without blocking
      CompletionStage<Boolean> stage = BlockHoundHelper.ensureNonBlocking(() ->
            scriptingManagerImpl.containsScriptAsync("nonExistent.js")
      );
      assertFalse(CompletionStages.join(stage));

      // scriptCache should now be initialized
      Cache<String, String> cache = TestingUtil.extractField(scriptingManagerImpl, "scriptCache");
      assertNotNull(cache);

      // Subsequent calls when already initialized should also succeed without blocking and without using blockingManager
      CompletionStage<Boolean> stage2 = BlockHoundHelper.ensureNonBlocking(() ->
            scriptingManagerImpl.containsScriptAsync("nonExistent.js")
      );
      assertFalse(CompletionStages.join(stage2));

      // With an existing script
      ScriptingUtils.loadScript(scriptingManager, TEST_SCRIPT);
      CompletionStage<Boolean> stage3 = BlockHoundHelper.ensureNonBlocking(() ->
            scriptingManagerImpl.containsScriptAsync(TEST_SCRIPT)
      );
      assertTrue(CompletionStages.join(stage3));

      // Reset scriptCache to null and run a task through TaskManager to verify end-to-end integration
      TestingUtil.replaceField(null, "scriptCache", scriptingManagerImpl, ScriptingManagerImpl.class);
      CompletionStage<String> taskStage = BlockHoundHelper.ensureNonBlocking(() ->
            taskManager.runTask(TEST_SCRIPT, new TaskContext().addParameter("a", "b"))
      );
      assertEquals("b", CompletionStages.join(taskStage));
   }
}
