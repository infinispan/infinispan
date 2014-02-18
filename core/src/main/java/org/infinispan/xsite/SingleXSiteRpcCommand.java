package org.infinispan.xsite;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SingleXSiteRpcCommand extends XSiteReplicateCommand {

   public static final int COMMAND_ID = 40;
   private VisitableCommand command;

   public SingleXSiteRpcCommand(String cacheName, VisitableCommand command) {
      super(cacheName);
      this.command = command;
   }

   public SingleXSiteRpcCommand(String cacheName) {
      super(cacheName);
   }

   public SingleXSiteRpcCommand() {
      super(null);
   }

   @Override
   public Object performInLocalSite(BackupReceiver receiver) throws Throwable {
      return receiver.handleRemoteCommand(command);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return command.perform(ctx);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{command};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Unusupported command id:" + commandId);
      }
      command = (VisitableCommand) parameters[0];
   }

   @Override
   public boolean isReturnValueExpected() {
      return command.isReturnValueExpected();
   }

   @Override
   public String toString() {
      return "SingleXSiteRpcCommand{" +
            "command=" + command +
            '}';
   }
}
