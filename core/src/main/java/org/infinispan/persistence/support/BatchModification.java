package org.infinispan.persistence.support;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.persistence.spi.MarshalledEntry;

/**
 * A simple wrapper class, necessary for Transactional stores, which allows MarshalledEntries and Object keys to be passed
 * to a store implementation in order. This class also removes repeated operations on the same key in order to prevent
 * redundant operations on the underlying store.  For example a tx, {put(1, "Test"); remove(1);}, will be simply written
 * to the store as {remove(1);}.
 *
 * @author Ryan Emerson
 */
public class BatchModification {
   private final Map<Object, MarshalledEntry> marshalledEntries = new HashMap<>();
   private final Set<Object> keysToRemove = new HashSet<>();
   private final Set<Object> affectedKeys;

   public BatchModification(Set<Object> affectedKeys) {
      this.affectedKeys = affectedKeys;
   }

   public void addMarshalledEntry(Object key, MarshalledEntry marshalledEntry) {
      keysToRemove.remove(key);
      marshalledEntries.put(key, marshalledEntry);
   }

   public void removeEntry(Object key) {
      marshalledEntries.remove(key);
      keysToRemove.add(key);
   }

   public Set<Object> getAffectedKeys() {
      return affectedKeys;
   }

   public Set<Object> getKeysToRemove() {
      return keysToRemove;
   }

   public Collection<MarshalledEntry> getMarshalledEntries() {
      return marshalledEntries.values();
   }
}
