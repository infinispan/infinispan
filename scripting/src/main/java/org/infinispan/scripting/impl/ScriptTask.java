package org.infinispan.scripting.impl;

import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskExecutionMode;

public class ScriptTask implements Task {

   private final String name;
   private final TaskExecutionMode mode;

   public ScriptTask(String name, TaskExecutionMode mode) {
      this.name = name;
      this.mode = mode;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String getType() {
      return "Script";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return mode;
   }

}
