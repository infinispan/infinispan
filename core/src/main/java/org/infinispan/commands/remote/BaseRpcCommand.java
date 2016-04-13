package org.infinispan.commands.remote;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

public abstract class BaseRpcCommand implements CacheRpcCommand {
   protected final ByteString cacheName;

   private Address origin;

   protected BaseRpcCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public ByteString getCacheName() {
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
