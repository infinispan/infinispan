package org.infinispan.xsite;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SingleXSiteRpcCommand extends XSiteReplicateCommand {

   public static final byte COMMAND_ID = 40;
   private VisitableCommand command;

   public SingleXSiteRpcCommand(ByteString cacheName, VisitableCommand command) {
      super(COMMAND_ID, cacheName);
      this.command = command;
   }

   public SingleXSiteRpcCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public SingleXSiteRpcCommand() {
      this(null);
   }

   @Override
   public CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      return receiver.handleRemoteCommand(command, preserveOrder);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(command);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
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
