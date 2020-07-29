package org.infinispan.xsite;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * RPC command to replicate cache rpc operations (i.e. not VisitableCommands) to the backup site.
 *
 * @author wburns
 * @since 12.0
 */
public class SingleXSiteCacheRpcCommand extends XSiteReplicateCommand<Object> {

   public static final byte COMMAND_ID = 40;
   private CacheRpcCommand command;

   public SingleXSiteCacheRpcCommand(ByteString cacheName, CacheRpcCommand command) {
      super(COMMAND_ID, cacheName);
      this.command = command;
   }

   public SingleXSiteCacheRpcCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public SingleXSiteCacheRpcCommand() {
      this(null);
   }

   @Override
   public CompletionStage<Object> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
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
      command = (CacheRpcCommand) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return command.isReturnValueExpected();
   }

   @Override
   public String toString() {
      return "SingleXSiteCacheRpcCommand{" +
            "command=" + command +
            '}';
   }
}
