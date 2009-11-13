package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.util.Immutables;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A container that maintains order of entries based on when they were placed in the container.  Iterators obtained from
 * this container maintain this order.
 * <p/>
 * This container offers constant-time operation for all public API methods.
 * <p/>
 * This is implemented using a set of lockable segments, each of which is a hash table, not unlike the JDK's {@link
 * java.util.concurrent.ConcurrentHashMap} with the exception that each entry is also linked.
 * <p/>
 * Links are maintained using techniques inspired by H. Sundell and P. Tsigas' 2008 paper, <a
 * href="http://www.md.chalmers.se/~tsigas/papers/Lock-Free-Deques-Doubly-Lists-JPDC.pdf"><i>Lock Free Deques and Doubly
 * Linked Lists</i></a>, M. Michael's 2002 paper, <a href="http://www.research.ibm.com/people/m/michael/spaa-2002.pdf"><i>High
 * Performance Dynamic Lock-Free Hash Tables and List-Based Sets</i></a>, and Java6's ConcurrentSkipListMap.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public class FIFODataContainer implements DataContainer {

   /**
    * The maximum capacity, used if a higher value is implicitly specified by either of the constructors with arguments.
    * MUST be a power of two <= 1<<30 to ensure that entries are indexable using ints.
    */
   static final int MAXIMUM_CAPACITY = 1 << 30;

   // -- these fields are all very similar to JDK's ConcurrentHashMap

   /**
    * Mask value for indexing into segments. The upper bits of a key's hash code are used to choose the segment.
    */
   final int segmentMask;

   /**
    * Shift value for indexing within segments.
    */
   final int segmentShift;

   /**
    * The segments, each of which is a specialized hash table
    */
   final Segment[] segments;

   Set<Object> keySet;

   final LinkedEntry head = new LinkedEntry(null), tail = new LinkedEntry(null);

   public FIFODataContainer(int concurrencyLevel) {
      float loadFactor = 0.75f;
      int initialCapacity = 256;

      // Find power-of-two sizes best matching arguments
      int sshift = 0;
      int ssize = 1;
      while (ssize < concurrencyLevel) {
         ++sshift;
         ssize <<= 1;
      }
      segmentShift = 32 - sshift;
      segmentMask = ssize - 1;
      this.segments = Segment.newArray(ssize);

      if (initialCapacity > MAXIMUM_CAPACITY)
         initialCapacity = MAXIMUM_CAPACITY;
      int c = initialCapacity / ssize;
      if (c * ssize < initialCapacity)
         ++c;
      int cap = 1;
      while (cap < c)
         cap <<= 1;

      for (int i = 0; i < this.segments.length; ++i) this.segments[i] = new Segment(cap, loadFactor);
      initLinks();
   }

   // links and link management

   /**
    * Back off
    *
    * @param nanos nanos to back off for.  If -1, starts at a default
    * @return next time, back off for these nanos
    */
   private static final long backoffStart = 10000;

   private long backoff(long nanos) {
      long actualNanos = nanos < 0 ? backoffStart : nanos;
      LockSupport.parkNanos(actualNanos);
      long newNanos = actualNanos << 1;
      return newNanos > 10000000 ? backoffStart : newNanos;
   }

   /**
    * Tests whether a given linked entry is marked for deletion.  In this implementation, being "marked" means that it
    * is of type Marker rather than LinkedEntry, but given the relative cost of an "instanceof" check, we prefer to test
    * the state of the InternalCacheEntry referenced by the LinkedEntry.  An InternalCacheEntry *always* exists so if it
    * is null, then this is a marker (or possibly the head or tail dummy entry).
    *
    * @param e entry to test
    * @return true if the entry is marked for removal.  False if it is not, or if the entry is the head or tail dummy
    *         entry.
    */
   protected final boolean isMarkedForRemoval(LinkedEntry e) {
      return e != head && e != tail && e.e == null;
   }

   /**
    * Places a removal marker the 'previous' reference on the given entry.  Note that marking a reference does not mean
    * that the reference pointed to is marked for removal, rather it means the LinkedEntry doing the referencing is the
    * entry to be removed.
    *
    * @param e entry
    * @return true if the marking was successful, false otherwise.  Could return false if the reference is already
    *         marked, or if the CAS failed.
    */
   protected final boolean markPrevReference(LinkedEntry e) {
      if (isMarkedForRemoval(e.p)) return false;
      Marker m = new Marker(e.p);
      return e.casPrev(e.p, m);
   }

   /**
    * Places a removal marker the 'next' reference on the given entry.  Note that marking a reference does not mean that
    * the reference pointed to is marked for removal, rather it means the LinkedEntry doing the referencing is the entry
    * to be removed.
    *
    * @param e entry
    * @return true if the marking was successful, false otherwise.  Could return false if the reference is already
    *         marked, or if the CAS failed.
    */
   protected final boolean markNextReference(LinkedEntry e) {
      if (isMarkedForRemoval(e.n)) return false;
      Marker m = new Marker(e.n);
      return e.casNext(e.n, m);
   }

   /**
    * The LinkedEntry class.  This entry is stored in the lockable Segments, and is also capable of being doubly
    * linked.
    */
   static class LinkedEntry {
      volatile InternalCacheEntry e;
      /**
       * Links to next and previous entries.  Needs to be volatile.
       */
      volatile LinkedEntry n, p;

      /**
       * CAS updaters for prev and next references
       */
      private static final AtomicReferenceFieldUpdater<LinkedEntry, LinkedEntry> N_UPDATER = AtomicReferenceFieldUpdater.newUpdater(LinkedEntry.class, LinkedEntry.class, "n");
      private static final AtomicReferenceFieldUpdater<LinkedEntry, LinkedEntry> P_UPDATER = AtomicReferenceFieldUpdater.newUpdater(LinkedEntry.class, LinkedEntry.class, "p");

      /**
       * LinkedEntries must always have a valid InternalCacheEntry.
       *
       * @param e internal cache entry
       */
      LinkedEntry(InternalCacheEntry e) {
         this.e = e;
      }

      final boolean casNext(LinkedEntry expected, LinkedEntry newValue) {
         return N_UPDATER.compareAndSet(this, expected, newValue);
      }

      final boolean casPrev(LinkedEntry expected, LinkedEntry newValue) {
         return P_UPDATER.compareAndSet(this, expected, newValue);
      }
   }

   /**
    * A marker.  If a reference in LinkedEntry (either to its previous or next entry) needs to be marked, it should be
    * CAS'd with an instance of Marker that points to the actual entry.  Typically this is done by calling {@link
    * FIFODataContainer#markNextReference(org.infinispan.container.FIFODataContainer.LinkedEntry)} or {@link
    * FIFODataContainer#markPrevReference(org.infinispan.container.FIFODataContainer.LinkedEntry)}
    */
   static final class Marker extends LinkedEntry {
      Marker(LinkedEntry actual) {
         super(null);
         n = actual;
         p = actual;
      }
   }

   /**
    * Initializes links to an empty container
    */
   protected final void initLinks() {
      head.n = tail;
      head.p = tail;
      tail.n = head;
      tail.p = head;
   }

   /**
    * Un-links an entry from the doubly linked list in a threadsafe, lock-free manner.  The entry is typically retrieved
    * using Segment#locklessRemove() after locking the Segment.
    *
    * @param entry entry to unlink
    */
   protected final void unlink(LinkedEntry entry) {
      if (entry == head || entry == tail) return;
      for (; ;) {
         LinkedEntry next = entry.n;
         if (isMarkedForRemoval(next)) return;
         LinkedEntry prev;
         if (markNextReference(entry)) {
            next = entry.n;
            while (true) {
               prev = entry.p;
               if (isMarkedForRemoval(prev) || markPrevReference(entry)) {
                  prev = entry.p;
                  break;
               }
            }
            prev = correctPrev(prev.p, next.n);
         }
      }
   }

   /**
    * Links a new entry at the end of the linked list.  Typically done when a put() creates a new entry, or if ordering
    * needs to be updated based on access.  If this entry already exists in the linked list, it should first be {@link
    * #unlink(org.infinispan.container.FIFODataContainer.LinkedEntry)}ed.
    *
    * @param entry entry to link at end
    */
   protected final void linkAtEnd(LinkedEntry entry) {
      LinkedEntry prev = tail.p;
      long backoffTime = -1;
      for (; ;) {
         entry.p = prev;
         entry.n = tail;
         if (prev.casNext(tail, entry)) break;
         prev = correctPrev(prev, tail);
         backoffTime = backoff(backoffTime);
      }

      backoffTime = -1;
      for (; ;) {
         LinkedEntry l1 = tail.p;
         if (isMarkedForRemoval(l1) || entry.n != tail) break;
         if (tail.casPrev(l1, entry)) {
            if (isMarkedForRemoval(entry.p)) correctPrev(entry, tail);
            break;
         }
         backoffTime = backoff(backoffTime);
      }
   }

   /**
    * Retrieves the next entry after a given entry, skipping marked entries accordingly.
    *
    * @param current current entry to inspect
    * @return the next valid entry, or null if we have reached the end of the list.
    */
   protected final LinkedEntry getNext(LinkedEntry current) {
      for (; ;) {
         if (current == tail) return null;
         LinkedEntry next = current.n;
         if (isMarkedForRemoval(next)) next = next.n;
         boolean marked = isMarkedForRemoval(next.n);
         if (marked && !isMarkedForRemoval(current.n)) {
            markPrevReference(next);
            current.casNext(next, next.n.n); // since next.n is a marker
            continue;
         }
         current = next;
         if (!marked && next != tail) return current;
      }
   }

   /**
    * Retrieves the previous entry befora a given entry, skipping marked entries accordingly.
    *
    * @param current current entry to inspect
    * @return the previous valid entry, or null if we have reached the start of the list.
    */
   protected final LinkedEntry getPrev(LinkedEntry current) {
      for (; ;) {
         if (current == head) return null;
         LinkedEntry prev = current.p;
         if (prev.n == current && !isMarkedForRemoval(current) && !isMarkedForRemoval(current.n)) {
            current = prev;
            if (current != head) return current;
         } else if (isMarkedForRemoval(current.n)) {
            current = getNext(current);
         } else {
            prev = correctPrev(prev, current);
         }
      }
   }

   /**
    * Correct 'previous' links.  This 'helper' function is used if unable to properly set previous pointers (due to a
    * concurrent update) and is used when traversing the list in reverse.
    *
    * @param suggestedPreviousEntry suggested previous entry
    * @param currentEntry           current entry
    * @return the actual valid, previous entry.  Links are also corrected in the process.
    */
   protected final LinkedEntry correctPrev(LinkedEntry suggestedPreviousEntry, LinkedEntry currentEntry) {
      LinkedEntry lastLink = null, link1, prev2;
      long backoffTime = -1;
      while (true) {
         link1 = currentEntry.p;
         if (isMarkedForRemoval(link1)) break;
         prev2 = suggestedPreviousEntry.n;
         if (isMarkedForRemoval(prev2)) {
            if (lastLink != null) {
               markPrevReference(suggestedPreviousEntry);
               lastLink.casNext(suggestedPreviousEntry, prev2.p);
               suggestedPreviousEntry = lastLink;
               lastLink = null;
               continue;
            }
            prev2 = suggestedPreviousEntry.p;
            suggestedPreviousEntry = prev2;
            continue;
         }

         if (prev2 != currentEntry) {
            lastLink = suggestedPreviousEntry;
            suggestedPreviousEntry = prev2;
            continue;
         }

         if (currentEntry.casPrev(link1, suggestedPreviousEntry)) {
            if (isMarkedForRemoval(suggestedPreviousEntry.p)) continue;
            break;
         }
         backoffTime = backoff(backoffTime);
      }
      return suggestedPreviousEntry;
   }


   /**
    * Similar to ConcurrentHashMap's hash() function: applies a supplemental hash function to a given hashCode, which
    * defends against poor quality hash functions.  This is critical because ConcurrentHashMap uses power-of-two length
    * hash tables, that otherwise encounter collisions for hashCodes that do not differ in lower or upper bits.
    */
   final int hashOld(int h) {
      // Spread bits to regularize both segment and index locations,
      // using variant of single-word Wang/Jenkins hash.
      h += (h << 15) ^ 0xffffcd7d;
      h ^= (h >>> 10);
      h += (h << 3);
      h ^= (h >>> 6);
      h += (h << 2) + (h << 14);
      return h ^ (h >>> 16);
   }

   /**
    * Use the objects built in hash to obtain an initial value, then use a second four byte hash to obtain a more
    * uniform distribution of hash values. This uses a <a href = "http://burtleburtle.net/bob/hash/integer.html">4-byte
    * (integer) hash</a>, which produces well distributed values even when the original hash produces thghtly clustered
    * values.
    * <p />
    * Contributed by akluge <a href-="http://www.vizitsolutions.com/ConsistentHashingCaching.html">http://www.vizitsolutions.com/ConsistentHashingCaching.html</a>
    */
   final int hash(int hash) {
      hash = (hash + 0x7ED55D16) + (hash << 12);
      hash = (hash ^ 0xc761c23c) ^ (hash >> 19);
      hash = (hash + 0x165667b1) + (hash << 5);
      hash = (hash + 0xd3a2646c) ^ (hash << 9);
      hash = (hash + 0xfd7046c5) + (hash << 3);
      hash = (hash ^ 0xb55a4f09) ^ (hash >> 16);

      return hash;
   }

   /**
    * Returns the segment that should be used for key with given hash
    *
    * @param hash the hash code for the key
    * @return the segment
    */
   final Segment segmentFor(int hash) {
      return segments[(hash >>> segmentShift) & segmentMask];
   }

   /**
    * ConcurrentHashMap list entry. Note that this is never exported out as a user-visible Map.Entry.
    * <p/>
    * Because the value field is volatile, not final, it is legal wrt the Java Memory Model for an unsynchronized reader
    * to see null instead of initial value when read via a data race.  Although a reordering leading to this is not
    * likely to ever actually occur, the Segment.readValueUnderLock method is used as a backup in case a null
    * (pre-initialized) value is ever seen in an unsynchronized access method.
    */
   static final class HashEntry {
      final Object key;
      final int hash;
      volatile LinkedEntry value;
      final HashEntry next;

      HashEntry(Object key, int hash, HashEntry next, LinkedEntry value) {
         this.key = key;
         this.hash = hash;
         this.next = next;
         this.value = value;
      }
   }

   /**
    * Very similar to a Segment in a ConcurrentHashMap
    */
   static final class Segment extends ReentrantLock {
      /**
       * The number of elements in this segment's region.
       */
      transient volatile int count;

      /**
       * The table is rehashed when its size exceeds this threshold. (The value of this field is always
       * <tt>(int)(capacity * loadFactor)</tt>.)
       */
      transient int threshold;

      /**
       * The per-segment table.
       */
      transient volatile HashEntry[] table;

      /**
       * The load factor for the hash table.  Even though this value is same for all segments, it is replicated to avoid
       * needing links to outer object.
       *
       * @serial
       */
      final float loadFactor;

      Segment(int initialCapacity, float lf) {
         loadFactor = lf;
         setTable(new HashEntry[initialCapacity]);
      }

      static final Segment[] newArray(int i) {
         return new Segment[i];
      }

      /**
       * Sets table to new HashEntry array. Call only while holding lock or in constructor.
       */
      final void setTable(HashEntry[] newTable) {
         threshold = (int) (newTable.length * loadFactor);
         table = newTable;
      }

      /**
       * Returns properly casted first entry of bin for given hash.
       */
      final HashEntry getFirst(int hash) {
         HashEntry[] tab = table;
         return tab[hash & (tab.length - 1)];
      }

      /**
       * Reads value field of an entry under lock. Called if value field ever appears to be null. This is possible only
       * if a compiler happens to reorder a HashEntry initialization with its table assignment, which is legal under
       * memory model but is not known to ever occur.
       */
      final LinkedEntry readValueUnderLock(HashEntry e) {
         lock();
         try {
            return e.value;
         } finally {
            unlock();
         }
      }

      /* Specialized implementations of map methods */

      final LinkedEntry get(Object key, int hash) {
         if (count != 0) { // read-volatile
            HashEntry e = getFirst(hash);
            while (e != null) {
               if (e.hash == hash && key.equals(e.key)) {
                  LinkedEntry v = e.value;
                  if (v != null)
                     return v;
                  return readValueUnderLock(e); // recheck
               }
               e = e.next;
            }
         }
         return null;
      }

      /**
       * This put is lockless.  Make sure you call segment.lock() first.
       */
      final LinkedEntry locklessPut(Object key, int hash, LinkedEntry value) {
         int c = count;
         if (c++ > threshold) // ensure capacity
            rehash();
         HashEntry[] tab = table;
         int index = hash & (tab.length - 1);
         HashEntry first = tab[index];
         HashEntry e = first;
         while (e != null && (e.hash != hash || !key.equals(e.key)))
            e = e.next;

         LinkedEntry oldValue;
         if (e != null) {
            oldValue = e.value;
            e.value = value;
         } else {
            oldValue = null;
            tab[index] = new HashEntry(key, hash, first, value);
            count = c; // write-volatile
         }
         return oldValue;
      }

      final void rehash() {
         HashEntry[] oldTable = table;
         int oldCapacity = oldTable.length;
         if (oldCapacity >= MAXIMUM_CAPACITY)
            return;

         /*
         * Reclassify nodes in each list to new Map.  Because we are
         * using power-of-two expansion, the elements from each bin
         * must either stay at same index, or move with a power of two
         * offset. We eliminate unnecessary node creation by catching
         * cases where old nodes can be reused because their next
         * fields won't change. Statistically, at the default
         * threshold, only about one-sixth of them need cloning when
         * a table doubles. The nodes they replace will be garbage
         * collectable as soon as they are no longer referenced by any
         * reader thread that may be in the midst of traversing table
         * right now.
         */

         HashEntry[] newTable = new HashEntry[oldCapacity << 1];
         threshold = (int) (newTable.length * loadFactor);
         int sizeMask = newTable.length - 1;
         for (int i = 0; i < oldCapacity; i++) {
            // We need to guarantee that any existing reads of old Map can
            //  proceed. So we cannot yet null out each bin.
            HashEntry e = oldTable[i];

            if (e != null) {
               HashEntry next = e.next;
               int idx = e.hash & sizeMask;

               //  Single node on list
               if (next == null)
                  newTable[idx] = e;

               else {
                  // Reuse trailing consecutive sequence at same slot
                  HashEntry lastRun = e;
                  int lastIdx = idx;
                  for (HashEntry last = next;
                       last != null;
                       last = last.next) {
                     int k = last.hash & sizeMask;
                     if (k != lastIdx) {
                        lastIdx = k;
                        lastRun = last;
                     }
                  }
                  newTable[lastIdx] = lastRun;

                  // Clone all remaining nodes
                  for (HashEntry p = e; p != lastRun; p = p.next) {
                     int k = p.hash & sizeMask;
                     HashEntry n = newTable[k];
                     newTable[k] = new HashEntry(p.key, p.hash, n, p.value);
                  }
               }
            }
         }
         table = newTable;
      }

      /**
       * This is a lockless remove.  Make sure you acquire locks using segment.lock() first.
       */
      final LinkedEntry locklessRemove(Object key, int hash) {
         int c = count - 1;
         HashEntry[] tab = table;
         int index = hash & (tab.length - 1);
         HashEntry first = tab[index];
         HashEntry e = first;
         while (e != null && (e.hash != hash || !key.equals(e.key)))
            e = e.next;

         LinkedEntry oldValue = null;
         if (e != null) {
            oldValue = e.value;
            // All entries following removed node can stay
            // in list, but all preceding ones need to be
            // cloned.
            HashEntry newFirst = e.next;
            for (HashEntry p = first; p != e; p = p.next)
               newFirst = new HashEntry(p.key, p.hash,
                                        newFirst, p.value);
            tab[index] = newFirst;
            count = c; // write-volatile

         }
         return oldValue;
      }

      /**
       * This is a lockless clear.  Ensure you acquire locks on the segment first using segment.lock().
       */
      final void locklessClear() {
         if (count != 0) {
            HashEntry[] tab = table;
            for (int i = 0; i < tab.length; i++)
               tab[i] = null;
            count = 0; // write-volatile
         }
      }
   }


   protected final class KeySet extends AbstractSet<Object> {
      public Iterator<Object> iterator() {
         return new KeyIterator();
      }

      public int size() {
         return FIFODataContainer.this.size();
      }
   }

   protected final class Values extends AbstractCollection<Object> {
      public Iterator<Object> iterator() {
         return new ValueIterator();
      }

      public int size() {
         return FIFODataContainer.this.size();
      }
   }

   protected final class EntrySet extends AbstractSet<InternalCacheEntry> {
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator();
      }

      public int size() {
         return FIFODataContainer.this.size();
      }
   }

   protected abstract class LinkedIterator {
      LinkedEntry current = head;

      public boolean hasNext() {
         if (current == tail) return false;
         current = getNext(current);
         return current != null;
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   protected final class EntryIterator extends LinkedIterator implements Iterator<InternalCacheEntry> {
      public InternalCacheEntry next() {
         return current.e;
      }
   }

   protected final class ImmutableEntryIterator extends LinkedIterator implements Iterator<InternalCacheEntry> {
      public InternalCacheEntry next() {
         return Immutables.immutableInternalCacheEntry(current.e);
      }
   }

   protected final class KeyIterator extends LinkedIterator implements Iterator<Object> {
      public Object next() {
         return current.e.getKey();
      }
   }

   protected final class ValueIterator extends LinkedIterator implements Iterator<Object> {
      public Object next() {
         return current.e.getValue();
      }
   }


   // ----------- PUBLIC API ---------------

   public InternalCacheEntry get(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      LinkedEntry le = s.get(k, h);
      InternalCacheEntry ice = null;
      if (le != null) {
         ice = le.e;
         if (isMarkedForRemoval(le)) unlink(le);
      }
      if (ice != null) {
         if (ice.isExpired()) {
            remove(k);
            ice = null;
         } else {
            ice.touch();
         }
      }
      return ice;
   }

   public InternalCacheEntry peek(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      LinkedEntry le = s.get(k, h);
      InternalCacheEntry ice = null;
      if (le != null) {
         ice = le.e;
         if (isMarkedForRemoval(le)) unlink(le);
      }
      return ice;
   }

   public void put(Object k, Object v, long lifespan, long maxIdle) {
      // do a normal put first.
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      s.lock();
      LinkedEntry le;
      boolean newEntry = false;
      try {
         le = s.get(k, h);
         InternalCacheEntry ice = le == null ? null : le.e;
         if (ice == null) {
            newEntry = true;
            ice = InternalEntryFactory.create(k, v, lifespan, maxIdle);
            // only update linking if this is a new entry
            le = new LinkedEntry(ice);
         } else {
            ice.setValue(v);
            ice = ice.setLifespan(lifespan).setMaxIdle(maxIdle);
            // need to do this anyway since the ICE impl may have changed
            le.e = ice;
         }

         s.locklessPut(k, h, le);

         if (newEntry) {
            linkAtEnd(le);
         }
      } finally {
         s.unlock();
      }
   }

   public boolean containsKey(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      LinkedEntry le = s.get(k, h);
      InternalCacheEntry ice = null;
      if (le != null) {
         ice = le.e;
         if (isMarkedForRemoval(le)) unlink(le);
      }
      if (ice != null) {
         if (ice.isExpired()) {
            remove(k);
            ice = null;
         }
      }

      return ice != null;
   }

   public InternalCacheEntry remove(Object k) {
      int h = hash(k.hashCode());
      Segment s = segmentFor(h);
      s.lock();
      InternalCacheEntry ice = null;
      LinkedEntry le;
      try {
         le = s.locklessRemove(k, h);
         if (le != null) {
            ice = le.e;
            unlink(le);
         }
      } finally {
         s.unlock();
      }

      if (ice == null || ice.isExpired())
         return null;
      else
         return ice;
   }

   public int size() {
      // approximate sizing is good enough
      int sz = 0;
      final Segment[] segs = segments;
      for (Segment s : segs) sz += s.count;
      return sz;
   }

   public void clear() {
      // This is expensive...
      // lock all segments
      for (Segment s : segments) s.lock();
      try {
         for (Segment s : segments) s.locklessClear();
         initLinks();
      } finally {
         for (Segment s : segments) s.unlock();
      }
   }

   public Set<Object> keySet() {
      if (keySet == null) keySet = new KeySet();
      return keySet;
   }

   public Collection<Object> values() {
      return new Values();
   }

   public Set<InternalCacheEntry> entrySet() {
      return new EntrySet();
   }

   public void purgeExpired() {
      for (InternalCacheEntry ice : this) {
         if (ice.isExpired()) remove(ice.getKey());
      }
   }

   public Iterator<InternalCacheEntry> iterator() {
      return new EntryIterator();
   }
}
