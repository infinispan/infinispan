package org.infinispan.xsite.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * It manages the keys changed in the local cluster and sends to all asynchronous backup configured.
 * <p>
 * The {@code lockOwner} is the last command (or transaction) who updated the key. It is used to detect conflicting
 * local updates while sending to the remote backups (sites).
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface IracManager {

   /**
    * Sets the {@code key} as changed by the {@code lockOwner}.
    *
    * @param segment   The key's segment.
    * @param key       The key changed.
    * @param lockOwner The lock owner who updated the key.
    */
   void trackUpdatedKey(int segment, Object key, Object lockOwner);

   /**
    * Sets all keys as removed.
    */
   void trackClear();

   /**
    * Sets the {@code key} as not changed and remove any tombstone related to it.
    * <p>
    * If {@code lockOwner} isn't the last one who updated the key, this method is a no-op.
    *
    * @param segment   The key's segment.
    * @param key       The key.
    * @param lockOwner The lock owner who updated the key.
    * @param tombstone The tombstone (can be {@code null}).
    */
   void cleanupKey(int segment, Object key, Object lockOwner, IracMetadata tombstone);

   /**
    * Notifies a topology changed.
    *
    * @param oldCacheTopology The old {@link CacheTopology}.
    * @param newCacheTopology The new {@link CacheTopology}.
    */
   void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology);

   /**
    * Requests the state stored in this instance for the given {@code segments}.
    *
    * @param origin   The requestor.
    * @param segments The segments requested.
    */
   void requestState(Address origin, IntSet segments);

   /**
    * Receives the state related to the {@code key}.
    *
    * @param segment   The key's segment.
    * @param key       The key modified.
    * @param lockOwner The last {@code lockOwner}.
    * @param tombstone The tombstone (can be {@code null})
    */
   void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone);

   /**
    * Checks if the given key is expired on all other sites. If the key is expired on all other sites this will return
    * true
    *
    * @param key The key to check if it is expired or not
    * @return Whether this key is expired on all other sites
    */
   CompletionStage<Boolean> checkAndTrackExpiration(Object key);
}
