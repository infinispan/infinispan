package org.infinispan.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

import java.util.Collection;

/**
 * Handles inbound state transfers.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateConsumer {

   CacheTopology getCacheTopology();

   boolean isStateTransferInProgress();

   boolean isStateTransferInProgressForKey(Object key);

   /**
    * Receive notification of topology changes. StateRequestCommands are issued for segments that are new to this member
    * and the segments that are no longer owned are discarded.
    *
    * @param cacheTopology
    * @param isRebalance
    */
   void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance);

   void applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks);

   /**
    * Cancels all incoming state transfers. The already received data is not discarded.
    * This is executed when the cache is shutting down.
    */
   void stop();

   /**
    * Receive notification of updated keys right before they are committed in DataContainer.
    *
    * @param key the key that is being modified
    */
   void addUpdatedKey(Object key);

   /**
    * Checks if a given key was updated by user code during state transfer (and consequently it is untouchable by state transfer).
    *
    * @param key the key to check
    * @return true if the key is known to be modified, false otherwise
    */
   boolean isKeyUpdated(Object key);

   /**
    * Run a callback only if the key was not updated by user code, and prevent user code from updating
    * the key while running the callback.
    *
    * @param key The key to check
    * @param callback The callback to run
    * @return {@code true} if the callback was executed, {@code false} otherwise.
    */
   boolean executeIfKeyIsNotUpdated(Object key, Runnable callback);

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   void stopApplyingState();

   /**
    * @return  true if this node has already received the first rebalance command
    */
   boolean ownsData();
}
