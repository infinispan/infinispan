package org.infinispan.configuration.global;

public class ShutdownConfiguration {

   private final ShutdownHookBehavior hookBehavior;
   
   ShutdownConfiguration(ShutdownHookBehavior hookBehavior) {
      this.hookBehavior = hookBehavior;
   }

   public ShutdownHookBehavior hookBehavior() {
      return hookBehavior;
   }

   @Override
   public String toString() {
      return "ShutdownConfiguration{" +
            "hookBehavior=" + hookBehavior +
            '}';
   }

}