package org.infinispan.scripting.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.spi.NonBlockingTaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * ScriptingTaskEngine.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class ScriptingTaskEngine implements NonBlockingTaskEngine {
   private final ScriptingManagerImpl scriptingManager;

   public ScriptingTaskEngine(ScriptingManagerImpl scriptingManager) {
      this.scriptingManager = scriptingManager;
   }

   @Override
   public String getName() {
      return "Script";
   }

   @Override
   public List<Task> getTasks() {
      List<Task> tasks = new ArrayList<>();
      scriptingManager.getScriptNames().forEach(s -> {
         ScriptMetadata scriptMetadata = scriptingManager.getScriptMetadata(s);
         tasks.add(new ScriptTask(s, scriptMetadata.mode().isClustered() ? TaskExecutionMode.ALL_NODES : TaskExecutionMode.ONE_NODE, scriptMetadata.parameters()));
      });

      return tasks;
   }

   @Override
   public CompletionStage<List<Task>> getTasksAsync() {
      BlockingManager blockingManager = scriptingManager.cacheManager.getGlobalComponentRegistry().getComponent(BlockingManager.class);
      return blockingManager.supplyBlocking(this::getTasks, "ScriptingTaskEngine - getTasksAsync");
   }

   @Override
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager) {
      return scriptingManager.<T>runScript(taskName, context)
            .toCompletableFuture();
   }

   @Override
   public boolean handles(String taskName) {
      return scriptingManager.containsScript(taskName);
   }

   @Override
   public CompletionStage<Boolean> handlesAsync(String taskName) {
      return scriptingManager.containsScriptAsync(taskName);
   }
}
