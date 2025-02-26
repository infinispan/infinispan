package org.infinispan.remoting.rpc;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public class SleepingCacheRpcCommand extends BaseRpcCommand {

   @ProtoField(2)
   final long sleepTime;

   public SleepingCacheRpcCommand(ByteString cacheName) {
      this(cacheName, 0);
   }

   @ProtoFactory
   public SleepingCacheRpcCommand(ByteString cacheName, long sleepTime) {
      super(cacheName);
      this.sleepTime = sleepTime;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      Thread.sleep(sleepTime);
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
