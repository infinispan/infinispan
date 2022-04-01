package org.infinispan.anchored.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Extend a {@link ReadCommittedEntry} with the key location.
 *
 * @author Dan Berindei
 * @since 11
 */
public class AnchoredReadCommittedEntry<K, V> extends ReadCommittedEntry<K, V> {
   private Address location;

   public AnchoredReadCommittedEntry(K key, V value, Metadata metadata) {
      super(key, value, metadata);
   }

   /**
    * @return The anchor location of the key, or {@code null} if the local node is the anchor location.
    */
   public Address getLocation() {
      return location;
   }

   /**
    * Save the anchor location of the key outside the regular metadata.
    *
    * That way the primary can use the regular metadata for notifications/indexing and still store the location
    */
   public void setLocation(Address location) {
      this.location = location;
   }

   public static void setMissingLocation(CacheEntry<?, ?> cacheEntry, Address location) {
      if (cacheEntry instanceof AnchoredReadCommittedEntry) {
         AnchoredReadCommittedEntry<?, ?> anchoredEntry = (AnchoredReadCommittedEntry<?, ?>) cacheEntry;
         if (anchoredEntry.getLocation() == null) {
            anchoredEntry.setLocation(location);
         }
      }
   }

   @Override
   public void commit(DataContainer<K,V> container) {
      if (isChanged() && !isRemoved()) {
         // Entry created/modified: only store the key location
         V newValue = location != null ? null : value;
         Metadata newMetadata = location != null ? new RemoteMetadata(location, null) : metadata;
         container.put(key, newValue, newMetadata);
      } else {
         super.commit(container);
      }
   }

   @Override
   public CompletionStage<Void> commit(int segment, InternalDataContainer<K, V> container) {
      if (isChanged() && !isRemoved()) {
         // Entry created/modified: only store the key location
         V newValue = location != null ? null : value;
         Metadata newMetadata = location != null ? new RemoteMetadata(location, null) : metadata;
         container.put(segment, key, newValue, newMetadata, internalMetadata, created, lastUsed);
         return CompletableFutures.completedNull();
      } else {
         return super.commit(segment, container);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "(" + Util.hexIdHashCode(this) + "){" +
             "key=" + toStr(key) +
             ", value=" + toStr(value) +
             ", isCreated=" + isCreated() +
             ", isChanged=" + isChanged() +
             ", isRemoved=" + isRemoved() +
             ", isExpired=" + isExpired() +
             ", skipLookup=" + skipLookup() +
             ", metadata=" + metadata +
             ", internalMetadata=" + internalMetadata +
             ", location=" + location +
             '}';
   }
}
