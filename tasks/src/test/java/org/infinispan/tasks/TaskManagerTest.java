package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.DummyTaskEngine.DummyTaskTypes;
import org.infinispan.tasks.impl.TaskManagerImpl;
import org.infinispan.tasks.logging.Messages;
import org.infinispan.tasks.spi.TaskEngine;
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

   @Test(expectedExceptions = IllegalStateException.class)
   public void testRegisterDuplicateEngine() {
      taskManager.registerTaskEngine(taskEngine);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testUnhandledTask() {
      taskManager.runTask("UnhandledTask", new TaskContext());
   }

   public void testStoredEngines() {
      Collection<TaskEngine> engines = taskManager.getEngines();
      assertEquals(1, engines.size());

      assertEquals(taskEngine.getName(), engines.iterator().next().getName());
   }

   public void testRunTask() throws InterruptedException, ExecutionException {
      memoryLogger.reset();
      CompletableFuture<String> okTask = taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK.name(), new TaskContext().logEvent(true));
      assertEquals("result", okTask.get());
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals(Messages.MESSAGES.taskSuccess(DummyTaskTypes.SUCCESSFUL_TASK.name()), memoryLogger.getMessage());
      assertEquals("result", memoryLogger.getDetail());
      assertEquals(EventLogCategory.TASKS, memoryLogger.getCategory());
      assertEquals(EventLogLevel.INFO, memoryLogger.getLevel());

      memoryLogger.reset();
      CompletableFuture<Object> koTask = taskManager.runTask(DummyTaskTypes.FAILING_TASK.name(), new TaskContext().logEvent(true));
      String message = koTask.handle((r, e) -> { return e.getCause().getMessage(); }).get();
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals("exception", message);
      assertEquals(Messages.MESSAGES.taskFailure(DummyTaskTypes.FAILING_TASK.name()), memoryLogger.getMessage());
      assertTrue(memoryLogger.getDetail().contains("java.lang.Exception: exception"));
      assertEquals(EventLogCategory.TASKS, memoryLogger.getCategory());
      assertEquals(EventLogLevel.ERROR, memoryLogger.getLevel());

      memoryLogger.reset();
      CompletableFuture<Object> slowTask = taskManager.runTask(DummyTaskTypes.SLOW_TASK.name(), new TaskContext().logEvent(true));
      Collection<TaskExecution> currentTasks = taskManager.getCurrentTasks();
      assertEquals(1, currentTasks.size());
      TaskExecution execution = currentTasks.iterator().next();
      assertEquals(DummyTaskTypes.SLOW_TASK.name(), execution.getName());

      List<Task> tasks = taskManager.getTasks();
      assertEquals(3, tasks.size());

      Task task = tasks.get(2);
      assertEquals(DummyTaskTypes.SLOW_TASK.name(), task.getName());
      assertEquals("Dummy", task.getType());
      assertEquals(TaskExecutionMode.ONE_NODE, task.getExecutionMode());

      taskEngine.getSlowTask().complete("slow");
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals("slow", slowTask.get());
   }
}
