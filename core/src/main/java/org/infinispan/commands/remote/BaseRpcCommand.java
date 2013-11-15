package org.infinispan.commands.remote;

import org.infinispan.remoting.transport.Address;

public abstract class BaseRpcCommand implements CacheRpcCommand {
   protected final String cacheName;

   private Address origin;

   protected BaseRpcCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
   
   @Override
   public Address getOrigin() {
	   return origin;
   }
   
   @Override
   public void setOrigin(Address origin) {
	   this.origin = origin;
   }

   @Override
   public boolean canBlock() {
      return false;
   }
}
