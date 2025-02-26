package org.infinispan.xsite;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * Component present on a backup site that manages the backup information and logic.
 *
 * @author Mircea Markus
 * @see ClusteredCacheBackupReceiver
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface BackupReceiver {

   <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command);

   /**
    * Updates the key with the value from a remote site.
    * <p>
    * If a conflict occurs, the update can be discarded.
    *
    * @param key          The key to update.
    * @param value        The new value.
    * @param metadata     The new {@link Metadata}.
    * @param iracMetadata The {@link IracMetadata} for conflict resolution.
    * @return A {@link CompletionStage} that is completed when the update is apply in the cluster or is discarded.
    */
   CompletionStage<Void> putKeyValue(Object key, Object value, Metadata metadata, IracMetadata iracMetadata);

   /**
    * Deletes the key.
    * <p>
    * This is a request from the remote site and the removal can be discarded if a conflict happens.
    *
    * @param key          The key to delete.
    * @param iracMetadata The {@link IracMetadata} for conflict resolution.
    * @param expiration   {@code true} if it is to remove an expired key.
    * @return A {@link CompletionStage} that is completed when the key is deleted or it is discarded.
    */
   CompletionStage<Void> removeKey(Object key, IracMetadata iracMetadata, boolean expiration);

   /**
    * Clears the cache.
    * <p>
    * This is not safe and it doesn't perform any conflict resolution.
    *
    * @return A {@link CompletionStage} that is completed when the cache is cleared.
    */
   CompletionStage<Void> clearKeys();

   /**
    * Touches an entry and returns if it was able to or not.
    *
    * @param key the key of the entry to touch
    * @return if the entry was touched
    */
   CompletionStage<Boolean> touchEntry(Object key);

   /**
    * It handles the state transfer state from a remote site. It is possible to have a single node applying the state or
    * forward the state to respective primary owners.
    */
   CompletionStage<Void> handleStateTransferState(List<XSiteState> chunk, long timeoutMs);

   /**
    * It handles starting or finishing, base on {@code startReceiving}, of the state transfer from a remote site.
    * <p>
    * The command must be broadcast to the entire cluster in which the cache exists.
    *
    * @param originSite     The remote site which is starting or finishing sending the state transfer.
    * @param startReceiving {@code true} if the {@code originSite} wants to start sending state.
    */
   CompletionStage<Void> handleStateTransferControl(String originSite, boolean startReceiving);
}
