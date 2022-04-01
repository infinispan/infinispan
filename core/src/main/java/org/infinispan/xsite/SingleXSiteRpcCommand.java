package org.infinispan.xsite;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SingleXSiteRpcCommand extends XSiteReplicateCommand<Object> {

   public static final byte COMMAND_ID = 40;
   private ReplicableCommand command;

   public SingleXSiteRpcCommand(ByteString cacheName, ReplicableCommand command) {
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
   public CompletionStage<Object> performInLocalSite(ComponentRegistry registry, boolean preserveOrder) {
      // Need to check VisitableCommand before CacheRpcCommand as PrepareCommand implements both but need to visit
      if (command instanceof VisitableCommand) {
         return super.performInLocalSite(registry, preserveOrder);
      } else {
         try {
            //noinspection unchecked
            return (CompletionStage<Object>) ((CacheRpcCommand) command).invokeAsync(registry);
         } catch (Throwable throwable) {
            return CompletableFutures.completedExceptionFuture(throwable);
         }
      }
   }

   @Override
   public CompletionStage<Object> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      return receiver.handleRemoteCommand((VisitableCommand) command, preserveOrder);
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
      command = (ReplicableCommand) input.readObject();
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
