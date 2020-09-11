package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * Abstract class to invoke RPC on the remote site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class XSiteReplicateCommand<O> extends BaseRpcCommand {

   private final byte commandId;
   protected String originSite;

   protected XSiteReplicateCommand(byte commandId, ByteString cacheName) {
      super(cacheName);
      this.commandId = commandId;
   }

   public CompletionStage<O> performInLocalSite(ComponentRegistry registry, boolean preserveOrder) {
      //by default, the command is executed against BackupReceiver.
      return performInLocalSite(registry.getBackupReceiver().running(), preserveOrder);
   }

   public abstract CompletionStage<O> performInLocalSite(BackupReceiver receiver, boolean preserveOrder);

   public void setOriginSite(String originSite) {
      this.originSite = originSite;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public byte getCommandId() {
      return commandId;
   }
}
