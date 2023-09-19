package org.infinispan.xsite;

import static org.infinispan.xsite.commands.remote.Ids.VISITABLE_COMMAND;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.commands.remote.XSiteCacheRequest;
import org.infinispan.xsite.commands.remote.XSiteRequest;

/**
 * RPC command to replicate cache operations (such as put, remove, replace, etc.) to the backup site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SingleXSiteRpcCommand extends XSiteCacheRequest<Object> {

   private VisitableCommand command;

   public SingleXSiteRpcCommand(ByteString cacheName, VisitableCommand command) {
      super(cacheName);
      this.command = command;
   }

   public SingleXSiteRpcCommand() {
      this(null, null);
   }

   @Override
   protected CompletionStage<Object> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return registry.getBackupReceiver().running().handleRemoteCommand(command);
   }

   @Override
   public byte getCommandId() {
      return VISITABLE_COMMAND;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(command);
      super.writeTo(output);
   }

   @Override
   public XSiteRequest<Object> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      command = (VisitableCommand) input.readObject();
      return super.readFrom(input);
   }

   @Override
   public String toString() {
      return "SingleXSiteRpcCommand{" +
            "command=" + command +
            '}';
   }
}
