package org.infinispan.tasks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.tasks.spi.TaskEngine;

public class DummyTaskEngine implements TaskEngine {

   static enum DummyTaskTypes {
      SUCCESSFUL_TASK_ONE, SUCCESSFUL_TASK_TWO, FAILING_TASK, SLOW_TASK
   }

   final Set<String> tasks;

   public DummyTaskEngine() {
      tasks = new HashSet<>();
      for (DummyTaskTypes type : DummyTaskTypes.values()) {
         tasks.add(type.toString());
      }
   }

   @Override
   public String getName() {
      return "Dummy";
   }

   @Override
   public List<Task> getTasks() {
      List<Task> taskDetails = new ArrayList<>();
      tasks.forEach(task -> {
         taskDetails.add(new DummyTask(task));
      });
      return taskDetails;
   }

   @Override
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context) {
      switch (DummyTaskTypes.valueOf(taskName)) {
      case SUCCESSFUL_TASK_ONE:
      case SUCCESSFUL_TASK_TWO:
         return (CompletableFuture<T>) CompletableFuture.completedFuture("result");
      case FAILING_TASK:
         CompletableFuture<T> f = new CompletableFuture<>();
         f.completeExceptionally(new Exception("Task failed"));
         return f;
      case SLOW_TASK:
         CompletableFuture<String> s = CompletableFuture.supplyAsync(() -> {
            try {
               Thread.sleep(1000);
            } catch (Exception e) {
            }
            return "slowresult";
         });
         return (CompletableFuture<T>) s;
      }
      throw new IllegalArgumentException();
   }

   @Override
   public boolean handles(String taskName) {
      return tasks.contains(taskName);
   }

}
