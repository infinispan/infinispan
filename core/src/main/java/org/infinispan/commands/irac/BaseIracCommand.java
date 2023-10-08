package org.infinispan.commands.irac;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

abstract class BaseIracCommand implements CacheRpcCommand {

   protected final ByteString cacheName;

   protected BaseIracCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   @ProtoField(1)
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void setOrigin(Address origin) {
      // no-op
   }

   @Override
   public Address getOrigin() {
      // Not needed
      return null;
   }
}
