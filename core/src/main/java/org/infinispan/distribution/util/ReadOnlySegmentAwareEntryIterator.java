package org.infinispan.distribution.util;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Iterator implementation that shows a read only view of the provided iterator by only
 * allowing values that map to a given segment using the provided consistent hash.
 * <p>
 * This iterator is specifically used with the {@link ReadOnlySegmentAwareEntrySet} so
 * that it will properly filter out entries by their key instead of by the entry instance
 * 
 * @author wburns
 * @since 7.2
 */
public class ReadOnlySegmentAwareEntryIterator<K, V> extends ReadOnlySegmentAwareIterator<Entry<K, V>> {

   public ReadOnlySegmentAwareEntryIterator(Iterator<Entry<K, V>> iter, ConsistentHash ch, Set<Integer> allowedSegments) {
      super(iter, ch, allowedSegments);
   }

   @Override
   protected boolean valueAllowed(Object obj) {
      if (obj instanceof Entry) {
         return super.valueAllowed(((Entry<?, ?>)obj).getKey());
      }
      return false;
   }
}
