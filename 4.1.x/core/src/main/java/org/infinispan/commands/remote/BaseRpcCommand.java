package org.infinispan.commands.remote;

public abstract class BaseRpcCommand implements CacheRpcCommand {
   protected String cacheName;

   protected BaseRpcCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   protected BaseRpcCommand() {
   }

   public String getCacheName() {
      return cacheName;
   }
}
