package org.infinispan.xsite.statetransfer;

import java.util.Collection;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.remoting.transport.Address;

/**
 * Interface to store the information about a single remote site for {@link XSiteStateProviderImpl}.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public interface XSiteStateProviderState {

   /**
    * Creates a new {@link XSiteStatePushTask} to do state transfer to remove site.
    *
    * @param originator The originator {@link Address} (node who initiated the state transfer).
    * @param provider   The {@link XSiteStateProvider} instance to notify when the {@link XSiteStatePushTask} finishes.
    * @return The {@link XSiteStatePushTask} instance. or {@code null} if a state transfer is already in progress.
    */
   XSiteStatePushTask createPushTask(Address originator, XSiteStateProvider provider);

   /**
    * Cancels any running state transfer.
    * <p>
    * If no state transfer is in progress, this method is a no-op.
    */
   void cancelTransfer();

   /**
    * @return {@code true} if a state transfer is in progress for this site.
    */
   boolean isSending();

   /**
    * Returns
    *
    * @param members The current cluster members list.
    * @return {@code true} if a state transfer is in progress and the originator is not in that {@link Collection}.
    */
   boolean isOriginatorMissing(Collection<Address> members);

   /**
    * @return {@code true} if the backup is configured to synchronous cross-site replication.
    */
   boolean isSync();

   /**
    * Factory for {@link XSiteStateProviderState} instances.
    *
    * @param config The {@link BackupConfiguration}.
    * @return The {@link XSiteStateProviderState} instance.
    */
   static XSiteStateProviderState fromBackupConfiguration(BackupConfiguration config) {
      return config.isSyncBackup() ? SyncProviderState.create(config) : AsyncProviderState.create(config);
   }

}
