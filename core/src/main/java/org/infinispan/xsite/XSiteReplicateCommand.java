package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;

/**
 * Abstract class to invoke RPC on the remote site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class XSiteReplicateCommand extends BaseRpcCommand {

   private String originSite;

   protected XSiteReplicateCommand(ByteString cacheName) {
      super(cacheName);
   }

   public abstract CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder);

   public String getOriginSite() {
      return originSite;
   }

   public void setOriginSite(String originSite) {
      this.originSite = originSite;
   }
}
