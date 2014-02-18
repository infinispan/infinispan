package org.infinispan.xsite;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Global component that holds all the {@link BackupReceiver}s within this CacheManager.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface BackupReceiverRepository {

   /**
    * Returns the local cache associated defined as backup for the provided remote (site, cache) combo, or throws an
    * exception if no such site is defined.
    * <p/>
    * Also starts the cache if not already stated; that is because the cache is needed for update after when this method
    * is invoked.
    */
   public BackupReceiver getBackupReceiver(String originSiteName, String cacheName);
}
