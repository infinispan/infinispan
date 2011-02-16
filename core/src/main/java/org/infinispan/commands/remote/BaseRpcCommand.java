package org.infinispan.commands.remote;

import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;

public abstract class BaseRpcCommand implements CacheRpcCommand {
   protected String cacheName;

   protected Configuration configuration;
   protected ComponentRegistry componentRegistry;

   public void injectComponents(Configuration configuration, ComponentRegistry componentRegistry) {
      this.configuration = configuration;
      this.componentRegistry = componentRegistry;
   }

   public Configuration getConfiguration() {
      return configuration;
   }

   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }


   protected BaseRpcCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   protected BaseRpcCommand() {
   }

   public String getCacheName() {
      return cacheName;
   }
}
