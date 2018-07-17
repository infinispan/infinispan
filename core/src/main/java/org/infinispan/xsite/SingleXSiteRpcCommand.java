package org.infinispan.xsite;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.util.ByteString;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SingleXSiteRpcCommand extends XSiteReplicateCommand {

   public static final int COMMAND_ID = 40;
   private VisitableCommand command;

   public SingleXSiteRpcCommand(ByteString cacheName, VisitableCommand command) {
      super(cacheName);
      this.command = command;
   }

   public SingleXSiteRpcCommand(ByteString cacheName) {
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
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return command.invokeAsync();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output) throws IOException {
      output.writeObject(command);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      command = (VisitableCommand) input.readObject();
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
