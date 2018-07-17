package org.infinispan.remoting.rpc;

import java.io.IOException;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.context.InvocationContext;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.util.ByteString;

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
   public Object perform(InvocationContext ctx) throws Throwable {
      Thread.sleep(sleepTime);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output) throws IOException {
      output.writeLong(sleepTime);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      sleepTime = input.readLong();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
