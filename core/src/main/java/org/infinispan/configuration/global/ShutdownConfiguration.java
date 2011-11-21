package org.infinispan.configuration.global;

public class ShutdownConfiguration {

   private final ShutdownHookBehavior hookBehavior;
   
   ShutdownConfiguration(ShutdownHookBehavior hookBehavior) {
      this.hookBehavior = hookBehavior;
   }

   public ShutdownHookBehavior hookBehavior() {
      return hookBehavior;
   }

}