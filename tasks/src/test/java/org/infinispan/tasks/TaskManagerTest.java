package org.infinispan.tasks;

import static org.testng.AssertJUnit.assertEquals;
import java.time.Period;
import java.util.List;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.DummyTaskEngine.DummyTaskTypes;
import org.infinispan.tasks.impl.TaskManagerImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "tasks.TaskManagerTest", groups = "functional")
public class TaskManagerTest extends SingleCacheManagerTest {

   protected TaskManagerImpl taskManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      taskManager = (TaskManagerImpl) cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
      taskManager.registerTaskEngine(new DummyTaskEngine());
   }

   public void testRunTask() {
      taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK_ONE.name(), new TaskContext());
      List<TaskEvent> events = taskManager.getTaskHistory(DummyTaskTypes.SUCCESSFUL_TASK_ONE.name(), Period.ofDays(1));
      assertEquals(1, events.size());
      TaskEvent event = events.get(0);
      assertEquals(TaskEventStatus.SUCCESS, event.getStatus());
      taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK_ONE.name(), new TaskContext());
      taskManager.runTask(DummyTaskTypes.SUCCESSFUL_TASK_TWO.name(), new TaskContext());
      events = taskManager.getTaskHistory(DummyTaskTypes.SUCCESSFUL_TASK_ONE.name(), Period.ofDays(1));
      assertEquals(2, events.size());

      taskManager.runTask(DummyTaskTypes.FAILING_TASK.name(), new TaskContext());
   }
}
