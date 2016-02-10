package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.DummyTaskEngine.DummyTaskTypes;
import org.infinispan.tasks.impl.TaskManagerImpl;
import org.infinispan.tasks.logging.Messages;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogManager;
import org.testng.annotations.Test;

@Test(testName = "tasks.TaskManagerTest", groups = "functional")
public class TaskManagerTest extends SingleCacheManagerTest {

   protected TaskManagerImpl taskManager;
   private DummyTaskEngine taskEngine;
   private MemoryEventLogger memoryLogger;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      GlobalComponentRegistry gcr = cacheManager.getGlobalComponentRegistry();
      taskManager = (TaskManagerImpl) gcr.getComponent(TaskManager.class);
      taskEngine = new DummyTaskEngine();
      taskManager.registerTaskEngine(taskEngine);
      memoryLogger = new MemoryEventLogger(cacheManager, gcr.getTimeService());
      gcr.getComponent(EventLogManager.class).replaceEventLogger(memoryLogger);
   }

   public void testRunTask() throws InterruptedException, ExecutionException {
      memoryLogger.reset();
      CompletableFuture<String> okTask = taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK.name(), new TaskContext());
      assertEquals("result", okTask.get());
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals(Messages.MESSAGES.taskSuccess(DummyTaskTypes.SUCCESSFUL_TASK.name()), memoryLogger.getMessage());
      assertEquals("result", memoryLogger.getDetail());
      assertEquals(EventLogCategory.TASKS, memoryLogger.getCategory());
      assertEquals(EventLogLevel.INFO, memoryLogger.getLevel());

      memoryLogger.reset();
      CompletableFuture<Object> koTask = taskManager.runTask(DummyTaskTypes.FAILING_TASK.name(), new TaskContext());
      String message = koTask.handle((r, e) -> { return e.getCause().getMessage(); }).get();
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals("exception", message);
      assertEquals(Messages.MESSAGES.taskFailure(DummyTaskTypes.FAILING_TASK.name()), memoryLogger.getMessage());
      assertTrue(memoryLogger.getDetail().contains("java.lang.Exception: exception"));
      assertEquals(EventLogCategory.TASKS, memoryLogger.getCategory());
      assertEquals(EventLogLevel.ERROR, memoryLogger.getLevel());

      memoryLogger.reset();
      CompletableFuture<Object> slowTask = taskManager.runTask(DummyTaskTypes.SLOW_TASK.name(), new TaskContext());
      Collection<TaskExecution> currentTasks = taskManager.getCurrentTasks();
      assertEquals(1, currentTasks.size());
      TaskExecution execution = currentTasks.iterator().next();
      assertEquals(DummyTaskTypes.SLOW_TASK.name(), execution.getName());
      taskEngine.slow.complete("slow");
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals("slow", slowTask.get());
   }
}
