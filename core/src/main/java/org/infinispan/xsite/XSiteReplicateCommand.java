package org.infinispan.xsite;

import org.infinispan.commands.remote.BaseRpcCommand;

/**
 * Abstract class to invoke RPC on the remote site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class XSiteReplicateCommand extends BaseRpcCommand {

   private String originSite;

   protected XSiteReplicateCommand(String cacheName) {
      super(cacheName);
   }

   public abstract Object performInLocalSite(BackupReceiver receiver) throws Throwable;

   public String getOriginSite() {
      return originSite;
   }

   public void setOriginSite(String originSite) {
      this.originSite = originSite;
   }
}
