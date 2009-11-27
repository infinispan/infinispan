package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import net.jcip.annotations.ThreadSafe;

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
 * Performance Dynamic Lock-Free Hash Tables and List-Based Sets</i></a>.
 * <p />
 * This implementation uses JDK {@link java.util.concurrent.atomic.AtomicMarkableReference}
 * to implement reference deletion markers.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@ThreadSafe
public class FIFOAMRDataContainer implements DataContainer {

   /*
      This implementation closely follows the pseudocode in Sundell and Tsigas' paper (Referred to as STP) for managing
      the lock-free, threadsafe doubly linked list.  AtomicMarkedReferences are used to implement the pointers referred
      to in the paper.
    */

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

   public FIFOAMRDataContainer(int concurrencyLevel) {
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
   Random r = new Random();
   private static final long backoffStart = 10000;

   private long backoff(long nanos) {
//      long actualNanos = nanos < 0 ? backoffStart : nanos;
//      LockSupport.parkNanos(actualNanos);
//      long newNanos = actualNanos << 1;
//      return newNanos > 10000000 ? backoffStart : newNanos;
      int millis = (1 + r.nextInt(9)) * 10;
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(millis));
      return -1;
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
//   protected final boolean isMarkedForRemoval(LinkedEntry e) {
//      return e != head && e != tail && e.e == null;
//   }

   /**
    * Places a removal marker the 'previous' reference on the given entry.  Note that marking a reference does not mean
    * that the reference pointed to is marked for removal, rather it means the LinkedEntry doing the referencing is the
    * entry to be removed.
    *
    * @param e entry
    * @return true if the marking was successful, false otherwise.  Could return false if the reference is already
    *         marked, or if the CAS failed.
    */
//   protected final boolean markPrevReference(LinkedEntry e) {
//      return !e.p.isMarked() && e.p.attemptMark(e.p.getReference(), true);
//   }

   /**
    * Places a removal marker the 'next' reference on the given entry.  Note that marking a reference does not mean that
    * the reference pointed to is marked for removal, rather it means the LinkedEntry doing the referencing is the entry
    * to be removed.
    *
    * @param e entry
    * @return true if the marking was successful, false otherwise.  Could return false if the reference is already
    *         marked, or if the CAS failed.
    */
//   protected final boolean markNextReference(LinkedEntry e) {
//      return !e.n.isMarked() && e.n.attemptMark(e.n.getReference(), true);
//   }

   /**
    * The LinkedEntry class.  This entry is stored in the lockable Segments, and is also capable of being doubly
    * linked.
    */
   static class LinkedEntry {
      volatile InternalCacheEntry e;
      /**
       * Links to next and previous entries.  Needs to be volatile.
       */
//      volatile LinkedEntry n, p;
      AtomicMarkableReference<LinkedEntry> n = new AtomicMarkableReference<LinkedEntry>(null, false),
            p = new AtomicMarkableReference<LinkedEntry>(null, false);

      /**
       * CAS updaters for prev and next references
       */
//      private static final AtomicReferenceFieldUpdater<LinkedEntry, LinkedEntry> N_UPDATER = AtomicReferenceFieldUpdater.newUpdater(LinkedEntry.class, LinkedEntry.class, "n");
//      private static final AtomicReferenceFieldUpdater<LinkedEntry, LinkedEntry> P_UPDATER = AtomicReferenceFieldUpdater.newUpdater(LinkedEntry.class, LinkedEntry.class, "p");

      /**
       * LinkedEntries must always have a valid InternalCacheEntry.
       *
       * @param e internal cache entry
       */
      LinkedEntry(InternalCacheEntry e) {
         this.e = e;
      }

//      final boolean casNext(LinkedEntry expected, LinkedEntry newValue) {
//         return n.compareAndSet(expected, newValue, false, false);
//      }
//
//      final boolean casPrev(LinkedEntry expected, LinkedEntry newValue) {
//         return p.compareAndSet(expected, newValue, false, false);
//      }
//
//      @Override
//      public String toString() {
//         return "E" + Integer.toHexString(System.identityHashCode(this));
//      }
   }

   /**
    * A marker.  If a reference in LinkedEntry (either to its previous or next entry) needs to be marked, it should be
    * CAS'd with an instance of Marker that points to the actual entry.  Typically this is done by calling {@link
    * FIFOAMRDataContainer#markNextReference(FIFOAMRDataContainer.LinkedEntry)} or {@link
    * FIFOAMRDataContainer#markPrevReference(FIFOAMRDataContainer.LinkedEntry)}
    */
//   static final class Marker extends LinkedEntry {
//      Marker(LinkedEntry actual) {
//         super(null);
//         n = actual;
//         p = actual;
//      }
//
//      @Override
//      public String toString() {
//         return "M" + Integer.toHexString(System.identityHashCode(this));
//      }
//   }

   /**
    * Initializes links to an empty container
    */
   protected final void initLinks() {
      head.n.set(tail, false);
      head.p.set(tail, false);
      tail.n.set(head, false);
      tail.p.set(head, false);
   }

   /**
    * Un-links an entry from the doubly linked list in a threadsafe, lock-free manner.  The entry is typically retrieved
    * using Segment#locklessRemove() after locking the Segment.
    *
    * @param node entry to unlink
    */
   // This corresponds to the Delete() function in STP
   protected final void unlink(LinkedEntry node) {
      if (node == head || node == tail) return;
      while (true) {
         AtomicMarkableReference<LinkedEntry> next = node.n;
         if (next.isMarked()) return;
         if (node.n.compareAndSet(next.getReference(), next.getReference(), false, true)) {
            AtomicMarkableReference<LinkedEntry> prev;
            while (true) {
               prev = node.p;
               if (prev.isMarked() || node.p.compareAndSet(prev.getReference(), prev.getReference(), false, true)) {
                  break;
               }
            }
            correctPrev(prev.getReference().p.getReference(), next.getReference());
         }
      }
   }

   /**
    * Links a new entry at the end of the linked list.  Typically done when a put() creates a new entry, or if ordering
    * needs to be updated based on access.  If this entry already exists in the linked list, it should first be {@link
    * #unlink(FIFOAMRDataContainer.LinkedEntry)}ed.
    *
    * @param node entry to link at end
    */
   // Corresponds to PushRight() in STP
   protected final void linkAtEnd(LinkedEntry node) {
      LinkedEntry next = tail;
      LinkedEntry prev = next.p.getReference();
      long backoffTime = -1;
      while (true) {
         node.p.set(prev, false);
         node.n.set(next, false);
         if (prev.n.compareAndSet(next, node, false, false)) break;
         prev = correctPrev(prev, next);
         backoffTime = backoff(backoffTime);
      }

      // PushEnd()
      backoffTime = -1;
      while (true) {
         AtomicMarkableReference<LinkedEntry> l1 = next.p;
         if (l1.isMarked() || (node.n.isMarked() || node.n.getReference() != next)) break;
         if (next.p.compareAndSet(l1.getReference(), node, false, false)) {
            if (node.p.isMarked()) correctPrev(node, next);
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
   // Corresponds to the Next() function in STP                                            pom
   protected final LinkedEntry getNext(LinkedEntry current) {
      while (true) {
         if (current == tail) return null;
         AtomicMarkableReference<LinkedEntry> next = current.n;
         boolean d = next.getReference().n.isMarked();
         if (d && (!current.n.isMarked() || current.n.getReference() != next.getReference())) {
            // set mark next.p
            next.getReference().p.attemptMark(next.getReference().p.getReference(), true);
            current.n.compareAndSet(next.getReference(), next.getReference().n.getReference(), false, false);
            continue;
         }

         current = next.getReference();
         if (!d && next.getReference() != tail) return current;
      }
   }

   /**
    * Correct 'previous' links.  This 'helper' function is used if unable to properly set previous pointers (due to a
    * concurrent update) and is used when traversing the list in reverse.
    *
    * @param prev           suggested previous entry
    * @param node           current entry
    * @return the actual valid, previous entry.  Links are also corrected in the process.
    */
   // Corresponds to CorrectPrev() in STP
   protected final LinkedEntry correctPrev(LinkedEntry prev, LinkedEntry node) {
      LinkedEntry lastLink = null;
      AtomicMarkableReference<LinkedEntry> link1, prev2;
      long backoffTime = -1;

      // holders to atomically retrieve ref + mark
      boolean[] markHolder = new boolean[1];
      LinkedEntry referenceHolder;

      while (true) {
         link1 = node.p;
         if (link1.isMarked()) break;

         prev2 = prev.n;
         if (prev2.isMarked()) {
            if (lastLink != null) {
               AtomicMarkableReference<LinkedEntry> prevP = prev.p;
               while (!prevP.attemptMark(prevP.getReference(), true)) {}
               lastLink.n.compareAndSet(prev, prev2.getReference(), lastLink.n.isMarked(), false);
               prev = lastLink;
               lastLink = null;
               continue;
            }
            prev2 = prev.p;
            prev = prev2.getReference();
            continue;
         }

         if (prev2.getReference() != node) {
            lastLink = prev;
            prev = prev2.getReference();
            continue;
         }

         referenceHolder = link1.get(markHolder);
         if (node.p.compareAndSet(referenceHolder, prev, markHolder[0], false)) {
            if (prev.p.isMarked()) continue;
            break;
         }
         backoffTime = backoff(backoffTime);
      }
      return prev;
   }

//   private LinkedEntry unmarkPrevIfNeeded(LinkedEntry e) {
//      if (isMarkedForRemoval(e)) return e.p;
//      else return e;
//   }


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
    * <p/>
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
         return FIFOAMRDataContainer.this.size();
      }
   }

   protected final class Values extends AbstractCollection<Object> {
      public Iterator<Object> iterator() {
         return new ValueIterator();
      }

      public int size() {
         return FIFOAMRDataContainer.this.size();
      }
   }

   protected final class EntrySet extends AbstractSet<InternalCacheEntry> {
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator();
      }

      public int size() {
         return FIFOAMRDataContainer.this.size();
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
      if (le != null) ice = le.e;
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
      if (le != null) ice = le.e;
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
      if (le != null) ice = le.e;
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