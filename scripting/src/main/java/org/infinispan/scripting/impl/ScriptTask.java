package org.infinispan.scripting.impl;

import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskExecutionMode;

import java.util.Set;

public class ScriptTask implements Task {

   private final String name;
   private final TaskExecutionMode mode;
   private final Set<String> parameters;

   ScriptTask(String name, TaskExecutionMode mode, Set<String> parameters) {
      this.name = name;
      this.mode = mode;
      this.parameters = parameters;
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

   @Override
   public Set<String> getParameters() {
      return parameters;
   }

}
