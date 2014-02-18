package org.infinispan.xsite;

import org.infinispan.commands.remote.BaseRpcCommand;

/**
 * Abstract class to invoke RPC on the remote site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class XSiteReplicateCommand extends BaseRpcCommand {

   protected XSiteReplicateCommand(String cacheName) {
      super(cacheName);
   }

   public abstract Object performInLocalSite(BackupReceiver receiver) throws Throwable;
}
