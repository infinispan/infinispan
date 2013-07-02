package org.infinispan.remoting.rpc;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;

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

   public SleepingCacheRpcCommand(String cacheName) {
      super(cacheName);
   }

   public SleepingCacheRpcCommand(String cacheName, long sleepTime) {
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
   public Object[] getParameters() {
      return new Object[]{sleepTime};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("This is not the command id we expect: " + commandId);
      }
      this.sleepTime = (Long) parameters[0];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
