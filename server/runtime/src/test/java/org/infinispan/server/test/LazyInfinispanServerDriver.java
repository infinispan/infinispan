package org.infinispan.server.test;

import java.util.function.Supplier;

public class LazyInfinispanServerDriver implements Supplier<InfinispanServerDriver> {

   private final InfinispanServerRuleConfigurationBuilder configurationBuilder;

   private InfinispanServerDriver serverDriver;

   public LazyInfinispanServerDriver(InfinispanServerRuleConfigurationBuilder configurationBuilder) {
      this.configurationBuilder = configurationBuilder;
   }

   @Override
   public InfinispanServerDriver get() {
      if (this.serverDriver == null) {
         // if two threads have serverDriver == null,
         synchronized (this) {
            // this prevent two instance
            if (this.serverDriver == null) {
               final InfinispanServerTestConfiguration configuration = configurationBuilder.build();
               this.serverDriver = configuration.runMode().newDriver(configuration);
            }
         }
      }
      return this.serverDriver;
   }
}
