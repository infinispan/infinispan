package org.infinispan.remoting.rpc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public class SleepingCacheRpcCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 125;
   private long sleepTime;

   public SleepingCacheRpcCommand() {
      super(null);
   }

   public SleepingCacheRpcCommand(ByteString cacheName) {
      super(cacheName);
   }

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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(sleepTime);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      sleepTime = input.readLong();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
