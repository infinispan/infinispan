package org.infinispan.xsite;

import org.infinispan.Cache;

/**
 * {@link org.infinispan.xsite.BackupReceiverRepository} delegator. Mean to be overridden. For test purpose only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BackupReceiverRepositoryDelegator implements BackupReceiverRepository {

   protected final BackupReceiverRepository delegate;

   protected BackupReceiverRepositoryDelegator(BackupReceiverRepository delegate) {
      this.delegate = delegate;
   }

   @Override
   public BackupReceiver getBackupReceiver(String originSiteName, String cacheName) {
      return delegate.getBackupReceiver(originSiteName, cacheName);
   }

   @Override
   public BackupReceiver getBackupReceiver(Cache<Object, Object> cache) {
      return delegate.getBackupReceiver(cache);
   }
}
