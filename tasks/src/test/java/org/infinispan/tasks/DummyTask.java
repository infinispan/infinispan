package org.infinispan.tasks;

public class DummyTask implements Task {

   private final String name;

   public DummyTask(String name) {
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String getType() {
      return "Dummy";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ONE_NODE;
   }

}
