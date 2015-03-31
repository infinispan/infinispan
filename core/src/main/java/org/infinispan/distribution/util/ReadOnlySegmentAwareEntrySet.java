package org.infinispan.distribution.util;

import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Iterator implementation that shows a read only view of the provided iterator by only
 * allowing values that map to a given segment using the provided consistent hash.
 * <p>
 * This iterator is used with specifically with the {@link ReadOnlySegmentAwareEntrySet}
 * to properly filter the entry by the key instead of the entry instance itself.
 * 
 * @author wburns
 * @since 7.2
 */
public class ReadOnlySegmentAwareEntrySet<K, V> extends ReadOnlySegmentAwareSet<Entry<K, V>> {

   public ReadOnlySegmentAwareEntrySet(Set<Entry<K, V>> set, ConsistentHash ch,
         Set<Integer> allowedSegments) {
      super(set, ch, allowedSegments);
   }

   @Override
   protected boolean valueAllowed(Object obj) {
      if (obj instanceof Entry) {
         return super.valueAllowed(((Entry<?, ?>)obj).getKey());
      }
      return false;
   }

   @Override
   public Iterator<Entry<K, V>> iterator() {
      return new ReadOnlySegmentAwareEntryIterator<>(delegate().iterator(), ch,
            allowedSegments);
   }
}
