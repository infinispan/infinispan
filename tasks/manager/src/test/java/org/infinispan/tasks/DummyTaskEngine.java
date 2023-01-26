package org.infinispan.tasks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

public class DummyTaskEngine implements TaskEngine {

   public enum DummyTaskTypes {
      SUCCESSFUL_TASK, PARAMETERIZED_TASK, FAILING_TASK, SLOW_TASK, CACHE_TASK
   }

   private final Set<String> tasks;
   private CompletableFuture<String> slow;

   public DummyTaskEngine() {
      tasks = new HashSet<>();
      for (DummyTaskTypes type : DummyTaskTypes.values()) {
         tasks.add(type.toString());
      }
      slow = new CompletableFuture<>();
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
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager) {
      switch (DummyTaskTypes.valueOf(taskName)) {
      case SUCCESSFUL_TASK:
         return (CompletableFuture<T>) CompletableFuture.completedFuture("result");
      case PARAMETERIZED_TASK:
         Map<String, ?> params = context.getParameters().get();
         return (CompletableFuture<T>) CompletableFuture.completedFuture(params.get("parameter"));
      case FAILING_TASK:
         CompletableFuture<T> f = new CompletableFuture<>();
         f.completeExceptionally(new Exception("exception"));
         return f;
      case SLOW_TASK:
         return (CompletableFuture<T>) slow;
      case CACHE_TASK:
         return (CompletableFuture<T>) CompletableFuture.completedFuture(context.getCache().get().getName());
      }
      throw new IllegalArgumentException();
   }

   public void setSlowTask(CompletableFuture<String> slow) {
      this.slow = slow;
   }

   public CompletableFuture<String> getSlowTask() {
      return slow;
   }

   @Override
   public boolean handles(String taskName) {
      return tasks.contains(taskName);
   }

}
