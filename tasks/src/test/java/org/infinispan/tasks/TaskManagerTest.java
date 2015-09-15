package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.DummyTaskEngine.DummyTaskTypes;
import org.infinispan.tasks.impl.TaskManagerImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "tasks.TaskManagerTest", groups = "functional")
public class TaskManagerTest extends SingleCacheManagerTest {

   protected TaskManagerImpl taskManager;
   private DummyTaskEngine taskEngine;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      taskManager = (TaskManagerImpl) cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
      taskEngine = new DummyTaskEngine();
      taskManager.registerTaskEngine(taskEngine);
   }

   public void testRunTask() throws InterruptedException, ExecutionException {
      CompletableFuture<String> okTask = taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK.name(), new TaskContext());
      assertEquals("result", okTask.get());
      assertEquals(0, taskManager.getCurrentTasks().size());
      CompletableFuture<Object> koTask = taskManager.runTask(DummyTaskTypes.FAILING_TASK.name(), new TaskContext());
      String message = koTask.handle((r, e) -> { return e.getCause().getMessage(); }).get();
      assertEquals(0, taskManager.getCurrentTasks().size());
      assertEquals("exception", message);
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
