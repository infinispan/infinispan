package org.infinispan.commons.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Lookup on a table without collisions will require only single access, if there are collisions it will be
 * limited to (number of collisions to particular bin + 1) and all those will lie in proximity (32 * reference size).
 * Inserts can be O(n) in the worst case when we have to rehash whole table or search through close-to-full for an empty
 * spot.
 * <p>
 * Not thread safe (though, look-ups are safe when there are no concurrent modifications).
 *
 * @see <a href="https://en.wikipedia.org/wiki/Hopscotch_hashing">Hopscotch hashing</a>
 */
public class HopscotchHashMap<K, V> extends ArrayMap<K, V> {
   private static final int H = 32;
   private static final int MAX_REHASH_CYCLES = 100;
   private int[] hopinfo;
   private int a, b, M, m, wM;
   private int mask;

   public HopscotchHashMap(int initialCapacity) {
      // round to nearest power of 2
      initialCapacity = Math.max(32, initialCapacity);
      wM = Integer.numberOfLeadingZeros(initialCapacity - 1);
      M = 32 - wM;
      m = 1 << M;
      mask = m - 1;
      randomizeFunction();
      keys = new Object[m];
      values = new Object[m];
      hopinfo = new int[m];
   }

   private void randomizeFunction() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      a = random.nextInt() | 1; // a should be odd
      b = random.nextInt() >>> M;
   }

   private int bin(int hashCode) {
      return ((a * hashCode) + b) >>> wM;
   }

   @Override
   public V get(Object key) {
      Objects.requireNonNull(key);
      int bin = bin(key.hashCode());
      // first, optimistically assume that the key is on its bin
      Object storedKey = keys[bin];
      if (storedKey != null && storedKey.equals(key)) {
         return (V) values[bin];
      }
      // we can ignore the first position since we already tested it
      int bininfo = hopinfo[bin] & ~1;
      // try to lookup equal key in neighbourhood
      while (bininfo != 0) {
         int offset = Integer.numberOfTrailingZeros(bininfo);
         int bo = (bin + offset) & mask;
         storedKey = keys[bo];
         if (storedKey.equals(key)) {
            return (V) values[bo];
         }
         bininfo = bininfo & ~(1 << offset);
      }
      return null;
   }

   @Override
   public V put(K key, V value) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      ++modCount;
      try {
         if (shouldGrow()) {
            // If the table is utilized close to its capacity the optimistic assumption
            // that we can read directly on bin does not hold.
            return rehashAndPutInternal(key, value);
         } else {
            return putInternal(key, value);
         }
      } catch (RehashException e) {
         return rehashAndPutInternal(key, value);
      }
   }

   private V rehashAndPutInternal(K key, V value) {
      Object[] oldKeys = keys;
      Object[] oldValues = values;
      for (int cycle = 0; cycle < MAX_REHASH_CYCLES; ++cycle) {
         randomizeFunction();
         try {
            rehash(oldKeys, oldValues);
            return putInternal(key, value);
         } catch (RehashException ignored) {
         }
      }
      throw new IllegalStateException("Did not manage to rehash table with " + size + " elements");
   }

   private void rehash(Object[] oldKeys, Object[] oldValues) throws RehashException {
      if (shouldGrow()) {
         m *= 2;
         mask = m - 1;
         ++M;
         --wM;
      }
      keys = new Object[m];
      values = new Object[m];
      if (m > hopinfo.length) {
         hopinfo = new int[m];
      } else {
         Arrays.fill(hopinfo, 0);
      }
      size = 0;
      for (int i = 0; i < oldKeys.length; ++i) {
         if (oldKeys[i] == null) {
            continue;
         }
         putInternal(oldKeys[i], oldValues[i]);
      }
   }

   private boolean shouldGrow() {
      // keep load factor < 0.5 if possible, and m <= 2^31
      return size * 2 >= m && wM > 1;
   }

   private V putInternal(Object key, Object value) throws RehashException {
      int bin = bin(key.hashCode());
      int bininfo = hopinfo[bin];
      // try to lookup equal key in neighbourhood
      while (bininfo != 0) {
         int offset = Integer.numberOfTrailingZeros(bininfo);
         int bo = (bin + offset) & mask;
         Object storedKey = keys[bo];
         if (storedKey.equals(key)) {
            Object prev = values[bo];
            values[bo] = value;
            return (V) prev;
         }
         bininfo = bininfo & ~(1 << offset);
      }
      if (keys[bin] == null) {
         keys[bin] = key;
         values[bin] = value;
         hopinfo[bin] = hopinfo[bin] | 1;
         ++size;
         return null;
      }
      int empty = (bin + 1) & mask;
      // linear probe search
      while (keys[empty] != null && empty != bin) {
         empty = (empty + 1) & mask;
      }
      if (empty == bin) {
         // there's no space in the table
         assert size == m;
         throw new RehashException();
      }
      for (;;) {
         // we'll reach with the head as far back as we can
         int head;
         if (empty > bin) {
            head = empty - H + 1;
            if (head <= bin) {
               break;
            }
         } else {
            head = (empty - H + 1) & mask;
         }
         int headinfo;
         int distance;
         do {
            headinfo = hopinfo[head];
            distance = (empty - head) & mask;
            // mask bits after empty
            headinfo &= (1 << distance) - 1;
            if (headinfo != 0) {
               break;
            }
            // if the current head cannot help, we'll move it one step closer to the empty slot
            head = (head + 1) & mask;
         } while (head != empty);
         if (head == empty) {
            // there's no element that can be moved
            // make sure that we don't have duplicity on the empty pos before rehash
            keys[empty] = null;
            values[empty] = null;
            throw new RehashException();
         }
         int offset = Integer.numberOfTrailingZeros(headinfo);
         int newEmpty = (head + offset) & mask;
         keys[empty] = keys[newEmpty];
         values[empty] = values[newEmpty];
         // reload headinfo because we have masked bits after empty
         headinfo = hopinfo[head];
         hopinfo[head] = (headinfo & ~(1 << offset)) | (1 << distance);
         empty = newEmpty;
      }
      keys[empty] = key;
      values[empty] = value;
      int offset = (empty - bin) & mask;
      hopinfo[bin] = hopinfo[bin] | 1 << offset;
      ++size;
      return null;
   }

   @Override
   public V remove(Object key) {
      Objects.requireNonNull(key);
      ++modCount;
      int bin = bin(key.hashCode());
      int bininfo = hopinfo[bin];
      int previnfo = bininfo;
      // try to lookup equal key in neighbourhood
      while (bininfo != 0) {
         int offset = Integer.numberOfTrailingZeros(bininfo);
         int bo = (bin + offset) & mask;
         Object storedKey = keys[bo];
         if (storedKey.equals(key)) {
            Object prev = values[bo];
            previnfo = previnfo & ~(1 << offset);
            if (previnfo != 0 && offset == 0) {
               // if this bin has more entries and this is the first position, try to optimize further lookups
               // by moving the entry to the first position
               offset = Integer.numberOfTrailingZeros(previnfo);
               bo = (bin + offset) & mask;
               keys[bin] = keys[bo];
               values[bin] = values[bo];
               previnfo = previnfo & ~(1 << offset) | 1;
            }
            keys[bo] = null;
            values[bo] = null;
            hopinfo[bin] = previnfo;
            --size;
            return (V) prev;
         }
         bininfo = bininfo & ~(1 << offset);
      }
      return null;
   }

   private static class RehashException extends Exception {
      public RehashException() {
         super(null, null, false, false);
      }
   }
}
