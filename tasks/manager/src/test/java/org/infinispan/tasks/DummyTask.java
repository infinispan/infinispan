package org.infinispan.tasks;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

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

   @Override
   public Set<String> getParameters() {
      if (DummyTaskEngine.DummyTaskTypes.PARAMETERIZED_TASK.name().equals(name)) {
         return Collections.singleton("parameter");
      } else {
         return Collections.emptySet();
      }
   }

   @Override
   public Optional<String> getAllowedRole() {
      return Optional.of("DummyRole");
   }
}
