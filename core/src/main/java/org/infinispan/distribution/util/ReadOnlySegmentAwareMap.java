package org.infinispan.distribution.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.util.AbstractDelegatingMap;

/**
 * Map implementation that shows a read only view of the provided entry by only allowing
 * entries whose key maps to a given segment using the provided consistent hash.
 * <p>
 * Any operation that would modify this map will throw an {@link UnsupportedOperationException}
 * <p>
 * This map is useful when you don't want to copy an entire map but only need to see
 * entries from the given segments.
 * <p>
 * Note many operations are not constant time when using this map.  The
 * {@link ReadOnlySegmentAwareMap#values} method is not supported as well. Please check\
 * the method you are using to see if it will perform differently than normally expected.
 * @author wburns
 * @since 7.2
 */
public class ReadOnlySegmentAwareMap<K, V> extends AbstractDelegatingMap<K, V> {

   protected final Map<K, V> map;
   protected final ConsistentHash ch;
   protected final Set<Integer> allowedSegments;

   protected ReadOnlySegmentAwareSet<K> segmentAwareKeySet;
   protected ReadOnlySegmentAwareEntrySet<K, V> segmentAwareEntrySet;

   public ReadOnlySegmentAwareMap(Map<K, V> map, ConsistentHash ch,
         Set<Integer> allowedSegments) {
      super();
      this.map = Collections.unmodifiableMap(map);
      this.ch = ch;
      this.allowedSegments = allowedSegments;
   }

   @Override
   protected Map<K, V> delegate() {
      return map;
   }

   protected boolean keyAllowed(Object key) {
      int segment = ch.getSegment(key);
      return allowedSegments.contains(segment);
   }

   @Override
   public boolean containsKey(Object key) {
      if (keyAllowed(key)) {
         return super.containsKey(key);
      }
      return false;
   }

   @Override
   public boolean containsValue(Object value) {
      for (Entry<K, V> entry : entrySet()) {
         if (value.equals(entry.getValue())) {
            return true;
         }
      }
      return false;
   }

   @Override
   public V get(Object key) {
      if (keyAllowed(key)) {
         return super.get(key);
      }
      return null;
   }

   @Override
   public Set<java.util.Map.Entry<K, V>> entrySet() {
      if (segmentAwareEntrySet == null) {
         segmentAwareEntrySet = new ReadOnlySegmentAwareEntrySet<>(delegate().entrySet(),
               ch, allowedSegments);
      }
      return segmentAwareEntrySet;
   }

   /**
    * Checks if the provided map is empty.  This is done by iterating over all of the keys
    * until it can find a key that maps to a given segment.
    * <p>
    * This method should always be preferred over checking the size to see if it is empty.
    * <p>
    * This time complexity for this method between O(1) to O(N).
    */
   @Override
   public boolean isEmpty() {
      Set<K> keySet = keySet();
      Iterator<K> iter = keySet.iterator();
      return !iter.hasNext();
   }

   @Override
   public Set<K> keySet() {
      if (segmentAwareKeySet == null) {
         segmentAwareKeySet = new ReadOnlySegmentAwareSet<>(
               super.keySet(), ch, allowedSegments);
      }
      return segmentAwareKeySet;
   }

   /**
    * Returns the size of the read only map.  This is done by iterating over all of the
    * keys counting all that are in the segments.
    * <p>
    * If you are using this method to verify if the map is empty, you should instead use
    * the {@link ReadOnlySegmentAwareEntryMap#isEmpty()} as it will perform better if the
    * size is only used for this purpose.
    * <p>
    * This time complexity for this method is always O(N).
    */
   @Override
   public int size() {
      Set<K> keySet = keySet();
      Iterator<K> iter = keySet.iterator();
      int count = 0;
      while (iter.hasNext()) {
         iter.next();
         count++;
      }
      return count;
   }

   /**
    * NOTE: this method is not supported.  Due to the nature of this map, we don't want
    * to copy the underlying value collection.  Thus almost any operation will require
    * O(N) and therefore this method is not provided.
    */
   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "ReadOnlySegmentAwareMap [map=" + map + ", ch=" + ch + 
            ", allowedSegments=" + allowedSegments + "]";
   }
}
