package org.infinispan.xsite;

import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.jgroups.protocols.relay.SiteAddress;

/**
 * Global component that holds all the {@link BackupReceiver}s within this CacheManager.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface BackupReceiverRepository {

   /**
    * Process an CacheRpcCommand received from a remote site.
    */
   public Object handleRemoteCommand(SingleRpcCommand cmd, SiteAddress src) throws Throwable;
}
