package org.infinispan.server.resp.hll.internal;

import static org.infinispan.server.resp.hll.internal.CompactSet.STORE_SIZE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * The {@link org.infinispan.server.resp.hll.HyperLogLog} explicit representation.
 * <p>
 * This implementation hashes a new element and stores the value in a set. Although it keeps the hashes, it is still
 * probabilistic, as a hash conflict is possible. And then again, since the whole hash value is stored, this structure
 * has a threshold size. After reaching the threshold, it should convert to a compact representation.
 * </p>
 */
@ThreadSafe
@ProtoTypeId(ProtoStreamTypeIds.RESP_HYPER_LOG_LOG_EXPLICIT)
public class ExplicitSet implements HLLRepresentation {

   private final Set<Long> storage;
   private final int threshold;

   public ExplicitSet() {
      // The threshold is calculated based on the number of entries the compact store has but divided by the hash size in bytes.
      // In other words, after this point the explicit representation starts using more memory than the compact format.
      this.threshold = 1 + (STORE_SIZE / Long.BYTES);
      this.storage = ConcurrentHashMap.newKeySet(this.threshold);
   }

   @ProtoFactory
   ExplicitSet(Set<Long> storage) {
      this();
      this.storage.addAll(storage);
   }

   @Override
   public boolean set(byte[] data) {
      long hash = Util.hash(data);
      return storage.add(hash);
   }

   @Override
   public long cardinality() {
      return storage.size();
   }

   public boolean needsMigration() {
      return storage.size() >= threshold;
   }

   public void migrate(CompactSet cs) {
      cs.readSource(storage);
   }

   @ProtoField(number = 1, collectionImplementation = HashSet.class)
   Collection<Long> storage() {
      return new HashSet<>(storage);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ExplicitSet that = (ExplicitSet) o;
      return Objects.equals(storage, that.storage);
   }

   @Override
   public int hashCode() {
      return Objects.hash(storage);
   }
}
