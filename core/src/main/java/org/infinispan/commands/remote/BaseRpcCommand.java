package org.infinispan.commands.remote;

import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;

public abstract class BaseRpcCommand implements CacheRpcCommand {
   protected String cacheName;

   protected Configuration configuration;
   protected ComponentRegistry componentRegistry;
   private Address origin;

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

   @Override
   public String toString() {
      return "BaseRpcCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
   
   public Address getOrigin() {
	   return origin;
   }
   
   public void setOrigin(Address origin) {
	   this.origin = origin;
   }
}
