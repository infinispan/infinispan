// Revision 1.120

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
/*
 * Modified for https://issues.jboss.org/browse/ISPN-3023
 * Includes ideas described in http://portal.acm.org/citation.cfm?id=1547428
 *
 */

package org.infinispan.commons.util.concurrent.jdk8backported;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.jdk8backported.StrippedConcurrentLinkedDeque.DequeNode;

import java.io.ObjectStreamField;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;

/**
 * A hash table supporting full concurrency of retrievals and
 * high expected concurrency for updates. This class obeys the
 * same functional specification as {@link java.util.Hashtable}, and
 * includes versions of methods corresponding to each method of
 * {@code Hashtable}. However, even though all operations are
 * thread-safe, retrieval operations do <em>not</em> entail locking,
 * and there is <em>not</em> any support for locking the entire table
 * in a way that prevents all access.  This class is fully
 * interoperable with {@code Hashtable} in programs that rely on its
 * thread safety but not on its synchronization details.
 *
 * <p>Retrieval operations (including {@code get}) generally do not
 * block, so may overlap with update operations (including {@code put}
 * and {@code remove}). Retrievals reflect the results of the most
 * recently <em>completed</em> update operations holding upon their
 * onset. (More formally, an update operation for a given key bears a
 * <em>happens-before</em> relation with any (non-null) retrieval for
 * that key reporting the updated value.)  For aggregate operations
 * such as {@code putAll} and {@code clear}, concurrent retrievals may
 * reflect insertion or removal of only some entries.  Similarly,
 * Iterators and Enumerations return elements reflecting the state of
 * the hash table at some point at or since the creation of the
 * iterator/enumeration.  They do <em>not</em> throw {@link
 * ConcurrentModificationException}.  However, iterators are designed
 * to be used by only one thread at a time.  Bear in mind that the
 * results of aggregate status methods including {@code size}, {@code
 * isEmpty}, and {@code containsValue} are typically useful only when
 * a map is not undergoing concurrent updates in other threads.
 * Otherwise the results of these methods reflect transient states
 * that may be adequate for monitoring or estimation purposes, but not
 * for program control.
 *
 * <p>The table is dynamically expanded when there are too many
 * collisions (i.e., keys that have distinct hash codes but fall into
 * the same slot modulo the table size), with the expected average
 * effect of maintaining roughly two bins per mapping (corresponding
 * to a 0.75 load factor threshold for resizing). There may be much
 * variance around this average as mappings are added and removed, but
 * overall, this maintains a commonly accepted time/space tradeoff for
 * hash tables.  However, resizing this or any other kind of hash
 * table may be a relatively slow operation. When possible, it is a
 * good idea to provide a size estimate as an optional {@code
 * initialCapacity} constructor argument. An additional optional
 * {@code loadFactor} constructor argument provides a further means of
 * customizing initial table capacity by specifying the table density
 * to be used in calculating the amount of space to allocate for the
 * given number of elements.  Also, for compatibility with previous
 * versions of this class, constructors may optionally specify an
 * expected {@code concurrencyLevel} as an additional hint for
 * internal sizing.  Note that using many keys with exactly the same
 * {@code hashCode()} is a sure way to slow down performance of any
 * hash table. To ameliorate impact, when keys are {@link Comparable},
 * this class may use comparison order among keys to help break ties.
 *
 * <p>A {@link Set} projection of a EquivalentConcurrentHashMapV8 may be created
 * (using {@link #newKeySet(int, Equivalence)} or {@link #newKeySet(int, int, Equivalence)}), or viewed
 * (using {@link #keySet(Object)} when only keys are of interest, and the
 * mapped values are (perhaps transiently) not used or all take the
 * same mapping value.
 *
 * <p>This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces.
 *
 * <p>Like {@link Hashtable} but unlike {@link HashMap}, this class
 * does <em>not</em> allow {@code null} to be used as a key or value.
 *
 * <p>ConcurrentHashMapV8s support a set of sequential and parallel bulk
 * operations that are designed
 * to be safely, and often sensibly, applied even with maps that are
 * being concurrently updated by other threads; for example, when
 * computing a snapshot summary of the values in a shared registry.
 * There are three kinds of operation, each with four forms, accepting
 * functions with Keys, Values, Entries, and (Key, Value) arguments
 * and/or return values. Because the elements of a EquivalentConcurrentHashMapV8
 * are not ordered in any particular way, and may be processed in
 * different orders in different parallel executions, the correctness
 * of supplied functions should not depend on any ordering, or on any
 * other objects or values that may transiently change while
 * computation is in progress; and except for forEach actions, should
 * ideally be side-effect-free. Bulk operations on {@link java.util.Map.Entry}
 * objects do not support method {@code setValue}.
 *
 * <ul>
 * <li> forEach: Perform a given action on each element.
 * A variant form applies a given transformation on each element
 * before performing the action.</li>
 *
 * <li> search: Return the first available non-null result of
 * applying a given function on each element; skipping further
 * search when a result is found.</li>
 *
 * <li> reduce: Accumulate each element.  The supplied reduction
 * function cannot rely on ordering (more formally, it should be
 * both associative and commutative).  There are five variants:
 *
 * <ul>
 *
 * <li> Plain reductions. (There is not a form of this method for
 * (key, value) function arguments since there is no corresponding
 * return type.)</li>
 *
 * <li> Mapped reductions that accumulate the results of a given
 * function applied to each element.</li>
 *
 * <li> Reductions to scalar doubles, longs, and ints, using a
 * given basis value.</li>
 *
 * </ul>
 * </li>
 * </ul>
 *
 * <p>These bulk operations accept a {@code parallelismThreshold}
 * argument. Methods proceed sequentially if the current map size is
 * estimated to be less than the given threshold. Using a value of
 * {@code Long.MAX_VALUE} suppresses all parallelism.  Using a value
 * of {@code 1} results in maximal parallelism by partitioning into
 * enough subtasks to fully utilize the {@link
 * ForkJoinPool#commonPool()} that is used for all parallel
 * computations. Normally, you would initially choose one of these
 * extreme values, and then measure performance of using in-between
 * values that trade off overhead versus throughput.
 *
 * <p>The concurrency properties of bulk operations follow
 * from those of EquivalentConcurrentHashMapV8: Any non-null result returned
 * from {@code get(key)} and related access methods bears a
 * happens-before relation with the associated insertion or
 * update.  The result of any bulk operation reflects the
 * composition of these per-element relations (but is not
 * necessarily atomic with respect to the map as a whole unless it
 * is somehow known to be quiescent).  Conversely, because keys
 * and values in the map are never null, null serves as a reliable
 * atomic indicator of the current lack of any result.  To
 * maintain this property, null serves as an implicit basis for
 * all non-scalar reduction operations. For the double, long, and
 * int versions, the basis should be one that, when combined with
 * any other value, returns that other value (more formally, it
 * should be the identity element for the reduction). Most common
 * reductions have these properties; for example, computing a sum
 * with basis 0 or a minimum with basis MAX_VALUE.
 *
 * <p>Search and transformation functions provided as arguments
 * should similarly return null to indicate the lack of any result
 * (in which case it is not used). In the case of mapped
 * reductions, this also enables transformations to serve as
 * filters, returning null (or, in the case of primitive
 * specializations, the identity basis) if the element should not
 * be combined. You can create compound transformations and
 * filterings by composing them yourself under this "null means
 * there is nothing there now" rule before using them in search or
 * reduce operations.
 *
 * <p>Methods accepting and/or returning Entry arguments maintain
 * key-value associations. They may be useful for example when
 * finding the key for the greatest value. Note that "plain" Entry
 * arguments can be supplied using {@code new
 * AbstractMap.SimpleEntry(k,v)}.
 *
 * <p>Bulk operations may complete abruptly, throwing an
 * exception encountered in the application of a supplied
 * function. Bear in mind when handling such exceptions that other
 * concurrently executing functions could also have thrown
 * exceptions, or would have done so if the first exception had
 * not occurred.
 *
 * <p>Speedups for parallel compared to sequential forms are common
 * but not guaranteed.  Parallel operations involving brief functions
 * on small maps may execute more slowly than sequential forms if the
 * underlying work to parallelize the computation is more expensive
 * than the computation itself.  Similarly, parallelization may not
 * lead to much actual parallelism if all processors are busy
 * performing unrelated tasks.
 *
 * <p>All arguments to all task methods must be non-null.
 *
 * <p><em>jsr166e note: During transition, this class
 * uses nested functional interfaces with different names but the
 * same forms as those expected for JDK8.</em>
 *
 * <p>This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 *
 * <b>NOTE</b>: This map has been tweaked so that equality and hash code
 * calculations are done based on a passed {@link org.infinispan.commons.equivalence.Equivalence} function
 * implementation for keys and values, as opposed to relying on their own
 * equals/hashCode/toString implementations. This is handy when using
 * key/values whose mentioned methods cannot be overriden, i.e. arrays,
 * and in situations where users want to avoid using wrapper objects.
 * To help with future revisions of this class, changes other than
 * constructor changes have been marked with 'EQUIVALENCE_MOD' comment.
 *
 * @since 1.5
 * @author Doug Lea
 * @author Galder Zamarreño
 * @author William Burns
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class BoundedEquivalentConcurrentHashMapV8<K,V> extends AbstractMap<K,V>
      implements ConcurrentMap<K,V>, PeekableMap<K, V>, Serializable, ParallelIterableMap<K, V> {
   private static final long serialVersionUID = 7249069246763182397L;

   static final Object NULL_VALUE = new Object();
   static final Log log = LogFactory.getLog(BoundedEquivalentConcurrentHashMapV8.class);

   // EVICTION STUFF
   
   public static <K, V> EvictionListener<K, V> getNullEvictionListener() {
      return new NullEvictionListener<>();
   }
   
   public interface EvictionListener<K, V> {

      void onEntryEviction(Map<K, V> evicted);

      void onEntryChosenForEviction(Entry<K, V> entry);

      void onEntryActivated(Object key);

      void onEntryRemoved(Entry<K, V> entry);
   }

   static long roundUpToNearest8(long size) {
      return (size + 7) & ~0x7;
   }

   public static final class NodeSizeCalculatorWrapper<K, V> extends AbstractEntrySizeCalculatorHelper<K, V> {
      private final EntrySizeCalculator<? super K, ? super V> calculator;
      private final long nodeAverageSize;

      public NodeSizeCalculatorWrapper(EntrySizeCalculator<? super K, ? super V> calculator) {
         this.calculator = calculator;
         // The node itself is an object and has a reference to its class
         long calculateNodeAverageSize = OBJECT_SIZE + POINTER_SIZE;
         // 6 variables in Node, 5 object references
         calculateNodeAverageSize += 5 * POINTER_SIZE;
         // 1: the int for the hash
         calculateNodeAverageSize += 4;
         // 2: Key actual size is ignored - defined by user
         // 3: NodeEquivalence is ignored as it is shared between all of the nodes
         // 4: Value actual size is ignored - defined by user
         // 5: We have a reference to another node so it is ignored
         // 6: EvictionEntry currently we only support LRU, so assume that node
         long lruNodeSize = calculateLRUNodeSize();
         nodeAverageSize = roundUpToNearest8(calculateNodeAverageSize) + lruNodeSize;
      }

      private long calculateLRUNodeSize() {
         // The lru node itself is an object and has a reference to its class
         long size = OBJECT_SIZE + POINTER_SIZE;
         // LRUNode has 2 object references in it and 1 boolean
         size += 2 * POINTER_SIZE;
         // 1: LRUNode has a pointer back to an internal node, so nothing is added
         // 2: LRUNode has a DequeNode
         long dequeNodeSize = calculateDequeNodeSize();
         // 3: LRUNode has a boolean
         size += 1;
         return roundUpToNearest8(size) + dequeNodeSize;
      }

      private long calculateDequeNodeSize() {
         // Deque node itself is object and has a reference to its class
         long size = OBJECT_SIZE + POINTER_SIZE;
         // Deque node has 3 references in it
         size += 3 * POINTER_SIZE;
         // 2 of the references are other deque nodes (ignored) and the other is pointing
         // back to the node itself (ignored)
         return roundUpToNearest8(size);
      }

      @Override
      public long calculateSize(K key, V value) {
         long result = calculator.calculateSize(key, value) + nodeAverageSize;
         if (result < 0) {
            throw new ArithmeticException("Size overflow!");
         }
         return result;
      }
   }

   private static final class SingleEntrySizeCalculator implements EntrySizeCalculator<Object, Object> {
      private final static SingleEntrySizeCalculator SINGLETON = new SingleEntrySizeCalculator();
      @Override
      public long calculateSize(Object key, Object value) {
         return 1;
      }
   }

   // We need the suppress warnings because Java doesn't do wildcard types well in another
   // type so we can't pass our map with types properly
   @SuppressWarnings({ "rawtypes", "unchecked" })
   private void notifyEvictionListener(Collection<Node<K, V>> evicted) {
      // piggyback listener invocation on callers thread outside lock
      if (evicted != null && !evicted.isEmpty()) {
         Map evictedCopy;
         if (evicted.size() == 1) {
            Node<K, V> evictedEntry = evicted.iterator().next();
            evictedCopy = singletonMap(evictedEntry.key, evictedEntry.val);
         } else {
            evictedCopy = new HashMap<K, V>(evicted.size());
            for (Node<K, V> he : evicted) {
               evictedCopy.put(he.key, he.val);
            }
            evictedCopy = unmodifiableMap(evictedCopy);
         }
         evictionListener.onEntryEviction(evictedCopy);
      }
   }

   static class NullEvictionListener<K, V> implements EvictionListener<K, V> {
      @Override
      public void onEntryEviction(Map<K, V> evicted) {
         // Do nothing.
      }
      @Override
      public void onEntryChosenForEviction(Entry<K, V> entry) {
         // Do nothing.
      }
      @Override
      public void onEntryActivated(Object key) {
         // Do nothing.
      }
      @Override
      public void onEntryRemoved(Entry<K, V> entry) {
         // Do nothing.
      }
   }

   public interface EvictionPolicy<K, V> {

      Node<K,V> createNewEntry(K key, int hash, Node<K,V> next, V value, 
            EvictionEntry<K, V> evictionEntry);

      TreeNode<K,V> createNewEntry(K key, int hash, TreeNode<K,V> next, 
            TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry);

      /**
       * Invoked to notify EvictionPolicy implementation that there has been an attempt to access
       * an entry in Segment, however that entry was not present in Segment.
       * <p>
       * Note that this method is always invoked holding a lock on the table and only
       * is raised when a write operation occurs where there wasn't a previous value
       *
       * @param e
       *            accessed entry in Segment
       */
      void onEntryMiss(Node<K,V> e, V value);

      /**
       * Invoked to notify EvictionPolicy implementation that an entry in Segment has been
       * accessed.
       * <p>
       * Note that this method is invoked without the lock protecting the entry and is raised
       * when there was found to be a value but it could be changed since we don't
       * hold the lock
       *
       * @param e
       *            accessed entry in Segment
       */
      void onEntryHitRead(Node<K, V> e, V value);

      /**
       * Invoked to notify EvictionPolicy implementation that an entry in Segment has been
       * accessed.
       * <p>
       * Note that this method is invoked with the lock protecting the entry and is raised
       * when there is a previous value
       *
       * @param e
       *            accessed entry in Segment
       */
      void onEntryHitWrite(Node<K, V> e, V value);

      /**
       * Invoked to notify EvictionPolicy implementation that an entry e has been removed from
       * Segment.
       * <p>
       * The lock will for sure be held when this invoked
       *
       * @param e
       *            removed entry in Segment
       */
      void onEntryRemove(Node<K, V> e);
      
      /**
       * This should be invoked after an operation that would cause an element to be added
       * to the map to make sure that no elements need evicting.
       * <p>
       * Note this is also invoked after a read hit.
       * <p>
       * This method is never invoked while holding a lock on any segment
       * 
       * @return the nodes that were evicted
       */
      Collection<Node<K, V>> findIfEntriesNeedEvicting();

      void onResize(long oldSize, long newSize);
   }

   static class NullEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
      private final NodeEquivalence<K, V> nodeEq;
      
      public NullEvictionPolicy(NodeEquivalence<K, V> nodeEq) {
         this.nodeEq = nodeEq;
      }

      @Override
      public void onEntryMiss(Node<K, V> e, V value) {
         // Do nothing.
      }

      @Override
      public void onEntryRemove(Node<K, V> e) {
         // Do nothing.
      }

      @Override
      public Node<K, V> createNewEntry(K key, int hash, Node<K, V> next, V value, 
            EvictionEntry<K, V> evictionEntry) {
         // No eviction passed in
         return new Node<K, V>(hash, nodeEq, key, value, next);
      }
      
      @Override
      public TreeNode<K, V> createNewEntry(K key, int hash, TreeNode<K, V> next, 
            TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry) {
         return new TreeNode<>(hash, nodeEq, key, value, next, parent, evictionEntry);
      }

      @Override
      public Set<Node<K, V>> findIfEntriesNeedEvicting() {
         return InfinispanCollections.emptySet();
      }

      @Override
      public void onEntryHitRead(Node<K, V> e, V value) {
         // Do nothing.
      }

      @Override
      public void onEntryHitWrite(Node<K, V> e, V value) {
         // Do nothing.
      }

      @Override
      public void onResize(long oldSize, long newSize) {
         // Do nothing.
      }
   }

   static class LRUNode<K, V> implements EvictionEntry<K, V> {

      private final Node<K, V> attachedNode;
      DequeNode<Node<K, V>> queueNode;
      boolean removed;

      public LRUNode(Node<K, V> item) {
         this.attachedNode = item;
      }

      @Override
      public K getKey() {
         return attachedNode.key;
      }
   }

   static class LRUEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
      final BoundedEquivalentConcurrentHashMapV8<K, V> map;
      final StrippedConcurrentLinkedDeque<Node<K, V>> deque = 
            new StrippedConcurrentLinkedDeque<Node<K,V>>();
      final long maxSize;
      final AtomicReference<SizeAndEvicting> currentSize = new AtomicReference<>(
            new SizeAndEvicting(0, 0));
      final EntrySizeCalculator<? super K, ? super V> sizeCalculator;
      final boolean countingMemory;

      static final long NODE_ARRAY_BASE_OFFSET = getUnsafe().arrayBaseOffset(Node[].class);
      static final long NODE_ARRAY_OFFSET = getUnsafe().arrayIndexScale(Node[].class);

      public LRUEvictionPolicy(BoundedEquivalentConcurrentHashMapV8<K, V> map, long maxSize,
            EntrySizeCalculator<? super K, ? super V> sizeCalculator, boolean countingMemory) {
         this.map = map;
         this.maxSize = maxSize;
         this.sizeCalculator = sizeCalculator;
         this.countingMemory = countingMemory;
         if (countingMemory) {
            sun.misc.Unsafe unsafe = getUnsafe();
            // We add the memory usage this eviction policy
            long evictionPolicySize = unsafe.ADDRESS_SIZE + unsafe.ARRAY_OBJECT_INDEX_SCALE;
            // we have 4 object references
            evictionPolicySize += unsafe.ARRAY_OBJECT_INDEX_SCALE * 4;
            // and a long and a boolean
            evictionPolicySize += 8 + 1;

            // We do a very slim approximation of how much the map itself takes up in
            // space irrespective of the elements
            long mapSize = unsafe.ADDRESS_SIZE + unsafe.ARRAY_OBJECT_INDEX_SCALE;
            // There are 2 array references to nodes
            mapSize += NODE_ARRAY_BASE_OFFSET * 2;
            // There is are 2 longs and 3 ints
            mapSize += 8 * 2 + 4 * 3;
            // Counter cell array
            mapSize += unsafe.arrayBaseOffset(CounterCell[].class);
            // there are 8 references to other objects in the map
            mapSize += unsafe.ADDRESS_SIZE * 8;
            incrementSizeEviction(currentSize, roundUpToNearest8(evictionPolicySize) +
                  roundUpToNearest8(mapSize), 0);
         }
      }

      @Override
      public void onEntryHitRead(Node<K, V> e, V value) {
         LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
         // We synchronize in case if multiple threads are hitting this entry at the same
         // time so we don't link it last twice
         synchronized (eviction) {
            // If the queue node is null it means we just added this value
            // (but onEntryMiss hasn't ran)
            if (eviction.queueNode != null && !eviction.removed) {
               Node<K, V> oldItem = eviction.queueNode.item;
               // Now set the item to null if possible - if we couldn't that means
               // that the entry was removed from the queue concurrently - let that win
               if (oldItem != null && eviction.queueNode.casItem(oldItem, null)) {
                  // this doesn't get unlinked if it was a tail of head here
                  deque.unlink(eviction.queueNode);

                  DequeNode<Node<K, V>> queueNode = new DequeNode<>(e);
                  eviction.queueNode = queueNode;
                  deque.linkLast(queueNode);
               }
            }
         }
      }

      @Override
      public void onEntryHitWrite(BoundedEquivalentConcurrentHashMapV8.Node<K,V> e, V value) {
         onEntryHitRead(e, value);
      }

      @Override
      public void onEntryMiss(Node<K, V> e, V value) {
         LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
         synchronized (eviction) {
            if (!eviction.removed) {
               // increment size here
               DequeNode<Node<K, V>> queueNode = new DequeNode<>(e);
               eviction.queueNode = queueNode;
               deque.linkLast(queueNode);
               incrementSizeEviction(currentSize, sizeCalculator.calculateSize(e.key, value), 0);
            }
         }
      }

      @Override
      public void onEntryRemove(Node<K, V> e) {
         LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
         synchronized (eviction) {
            if (eviction.queueNode != null) {
               Node<K, V> item = eviction.queueNode.item;
               if (item != null && eviction.queueNode.casItem(item, null)) {
                  deque.unlink(eviction.queueNode);
               }
               eviction.queueNode = null;
            }
            // This is just in case if there are concurrent removes for the same key
            if (!eviction.removed) {
               eviction.removed = true;
               incrementSizeEviction(currentSize, -sizeCalculator.calculateSize(e.key, e.val), 0);
            }
         }
      }

      @Override
      public Node<K, V> createNewEntry(K key, int hash, Node<K, V> next, V value,
            EvictionEntry<K, V> evictionEntry) {
         Node<K, V> node = new Node<K, V>(hash, map.nodeEq, key, value, next);
         if (evictionEntry == null) {
            node.lazySetEviction(new LRUNode<>(node));
         } else {
            node.lazySetEviction(evictionEntry);
         }
         return node;
      }

      @Override
      public TreeNode<K, V> createNewEntry(K key, int hash, TreeNode<K, V> next, 
            TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry) {
         TreeNode<K, V> treeNode;
         if (evictionEntry == null) {
            treeNode = new TreeNode<>(hash, map.nodeEq, key, value, next, parent, null);
            treeNode.lazySetEviction(new LRUNode<>(treeNode));
         } else {
            treeNode = new TreeNode<>(hash, map.nodeEq, key, value, next, parent,
                  evictionEntry);
         }
         return treeNode;
      }

      @Override
      public Collection<Node<K, V>> findIfEntriesNeedEvicting() {

         long extra;
         while (true) {
            SizeAndEvicting existingSize = currentSize.get();
            long size = existingSize.size;
            long evicting = existingSize.evicting;
            // If there are extras then we need to increase eviction
            if ((extra = size - evicting - maxSize) > 0) {
               SizeAndEvicting newSize = new SizeAndEvicting(size, evicting + extra);
               if (currentSize.compareAndSet(existingSize, newSize)) {
                  break;
               }
            } else {
               break;
            }
         }
         List<Node<K, V>> evictedEntries = null;
         if (extra > 0) {
            evictedEntries = new ArrayList<>((int)extra & 0x7fffffff);
            long decCreate = 0;
            while (decCreate < extra) {
               Node<K, V> node = deque.pollFirst();
               boolean removed = false;
               if (node != null) {
                  LRUNode<K, V> lruNode = (LRUNode<K, V>) node.eviction;
                  synchronized (lruNode) {
                     if (!lruNode.removed) {
                        lruNode.removed = true;
                        removed = true;
                     }
                  }
               }

               if (removed) {
                  V value = map.replaceNode(node.key, null, null, true);
                  if (value != null) {
                     evictedEntries.add(node);
                     decCreate += sizeCalculator.calculateSize(node.key, value);
                  }
               } else {
                  // This basically means there was a concurrent remove, in which case
                  // we can't know how large it was so our eviction can't be correct.
                  // In this case break out and let the next person fix the eviction
                  break;
               }
            }
            // It is possible that decCreate is higher than extra and if the size calculation
            // can return a number greater than 1 most likely to occur very often
            incrementSizeEviction(currentSize, -decCreate, -extra);
         } else {
            evictedEntries = InfinispanCollections.emptyList();
         }

         return evictedEntries;
      }

      @Override
      public void onResize(long oldSize, long newSize) {
         if (countingMemory && newSize > oldSize) {
            // Need to increment the overall size
            incrementSizeEviction(currentSize, (newSize - oldSize) * NODE_ARRAY_OFFSET, 0);
         }
      }
   }

   enum Recency {
      HIR_RESIDENT, LIR_RESIDENT, HIR_NONRESIDENT, EVICTING, EVICTED, REMOVED
   }

   static interface EvictionEntry<K, V> {
      public K getKey();
   }

   static final class LIRSNode<K, V> implements EvictionEntry<K, V> {
      // The next few variables are to always be protected by "this" object monitor
      Recency state;
      DequeNode<LIRSNode<K, V>> stackNode;
      DequeNode<LIRSNode<K, V>> queueNode;
      boolean created;
      final K key;

      public LIRSNode(K key) {
         this.key = key;
      }

      public void setState(Recency recency) {
         state = recency;
      }

      public void setStackNode(DequeNode<LIRSNode<K, V>> stackNode) {
         this.stackNode = stackNode;
      }

      public void setQueueNode(DequeNode<LIRSNode<K, V>> queueNode) {
         this.queueNode = queueNode; 
      }

      @Override
      public String toString() {
         return "LIRSNode [state=" + state + ", stackNode=" + 
               System.identityHashCode(stackNode) + ", queueNode=" + 
               System.identityHashCode(queueNode)
               + ", key=" + key + "]";
      }

      @Override
      public K getKey() {
         return key;
      }
   }

   static final class SizeAndEvicting {
      private final long size;
      private final long evicting;

      public SizeAndEvicting(long size, long evicting) {
         this.size = size;
         this.evicting = evicting;
      }

      @Override
      public String toString() {
         return "SizeAndEvicting [size=" + size + ", evicting=" + evicting + "]";
      }
   }

   static SizeAndEvicting incrementSizeEviction(AtomicReference<SizeAndEvicting> currentSize, 
         long size, long eviction) {
      boolean replaced = false;
      SizeAndEvicting lirsSize = null;
      while (!replaced) {
         SizeAndEvicting existingSize = currentSize.get();
         long newSize = existingSize.size + size;
         long newEviction = existingSize.evicting + eviction;
         lirsSize = new SizeAndEvicting(newSize, newEviction);
         replaced = currentSize.compareAndSet(existingSize, lirsSize);
      }
      return lirsSize;
   }

   static final class LIRSEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
      /**
       * The percentage of the cache which is dedicated to hot blocks. See section 5.1
       */
      private static final float L_LIRS = 0.95f;

      final BoundedEquivalentConcurrentHashMapV8<K, V> map;
      // LIRS stack S
      /**
       * The LIRS stack, S, which is maintains recency information. All hot entries are on the
       * stack. All cold and non-resident entries which are more recent than the least recent hot
       * entry are also stored in the stack (the stack is always pruned such that the last entry is
       * hot, and all entries accessed more recently than the last hot entry are present in the
       * stack). The stack is ordered by recency, with its most recently accessed entry at the top,
       * and its least recently accessed entry at the bottom.
       */
      final StrippedConcurrentLinkedDeque<LIRSNode<K, V>> stack = new StrippedConcurrentLinkedDeque<>();
      // LIRS queue Q
      /**
       * The LIRS queue, Q, which enqueues all cold entries for eviction. Cold entries (by
       * definition in the queue) may be absent from the stack (due to pruning of the stack). Cold
       * entries are added to the end of the queue and entries are evicted from the front of the
       * queue.
       */
      final StrippedConcurrentLinkedDeque<LIRSNode<K, V>> queue = new StrippedConcurrentLinkedDeque<>();

      /** The maximum number of hot entries (L_lirs in the paper). */
      private final long maximumHotSize;

      /** The maximum number of resident entries (L in the paper). */
      private final long maximumSize;

      /** The actual number of hot entries. */
      private final AtomicLong hotSize = new AtomicLong();

      /**
       * This stores how many LIR to HIR demotions we need to do to keep in a consistent
       * state.  Note this should only be incremented when you are promoting a HIR to
       * LIR usually.
       */
      private final AtomicLong hotDemotion = new AtomicLong();

      private final AtomicReference<SizeAndEvicting> currentSize = new AtomicReference<>(
            new SizeAndEvicting(0, 0));

      final ThreadLocal<Collection<LIRSNode<K, V>>> nodesToEvictTL = new ThreadLocal<>();

      public LIRSEvictionPolicy(BoundedEquivalentConcurrentHashMapV8<K, V> map, long maxSize) {
         this.map = map;
         this.maximumSize = maxSize;
         this.maximumHotSize = calculateLIRSize(maxSize);
      }

      private static long calculateLIRSize(long maximumSize) {
         long result = (long) (L_LIRS * maximumSize);
         return (result == maximumSize) ? maximumSize - 1 : result;
      }

      @Override
      public Node<K, V> createNewEntry(K key, int hash, Node<K, V> next, V value,
            EvictionEntry<K, V> evictionEntry) {
         Node<K, V> node = new Node<K, V>(hash, map.nodeEq, key, value, next);
         if (evictionEntry == null) {
            node.lazySetEviction(new LIRSNode<K, V>(key));
         } else {
            node.lazySetEviction(evictionEntry);
         }
         return node;
      }

      @Override
      public TreeNode<K, V> createNewEntry(K key, int hash, TreeNode<K, V> next,
            TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry) {
         TreeNode<K, V> treeNode;
         if (evictionEntry == null) {
            treeNode = new TreeNode<>(hash, map.nodeEq, key, value, next, parent, null);
            treeNode.lazySetEviction(new LIRSNode<K, V>(key));
         } else {
            // We need to link the eviction entry with the new node now too
            treeNode = new TreeNode<>(hash, map.nodeEq, key, value, next, parent, evictionEntry);
         }
         return treeNode;
      }

      /**
       * Adds this LIRS node as LIR if there is room.
       * The lock must be obtained on the node to ensure consistency
       * @param lirsNode The node to try to add
       * @return if the node was added or not
       */
      boolean addToLIRIfNotFullHot(LIRSNode<K, V> lirsNode, boolean incrementSize) {
         long currentHotSize;
         // If we haven't hit LIR max then promote it immediately
         // See section 3.3 second paragraph:
         while ((currentHotSize = hotSize.get()) < maximumHotSize) {
            if (hotSize.compareAndSet(currentHotSize, currentHotSize + 1)) {
               if (incrementSize) {
                  incrementSizeEviction(currentSize, 1, 0);
               }
               DequeNode<LIRSNode<K, V>> stackNode = new DequeNode<>(lirsNode);
               lirsNode.setStackNode(stackNode);
               lirsNode.setState(Recency.LIR_RESIDENT);
               stack.linkLast(stackNode);
               return true;
            }
         }
         return false;
      }

      @Override
      public void onEntryMiss(Node<K, V> e, V value) {
         boolean pruneLIR = false;
         boolean evictHIR = false;
         boolean skipIncrement;
         LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
         synchronized (lirsNode) {
            // This tells us if this is the first time we hit a miss for this node.
            // This is important to tell if a node was hit and subsequently evicted
            // before our miss occurred
            skipIncrement = lirsNode.created;
            // Now set it to created
            lirsNode.created = true;
            Recency recency = lirsNode.state;
            // See section 3.3 case 3:
            if (recency == null) {
               if (skipIncrement) {
                  throw new IllegalStateException("Created should always be false for a"
                        + "newly created evictio node!");
               }

               // This should be the most common case by far 
               // (alreadyCreated is implied to be false)
               // If it was added to LIR due to size don't do anymore
               if (addToLIRIfNotFullHot(lirsNode, true)) {
                  return;
               }

               // This is the (b) example
               lirsNode.setState(Recency.HIR_RESIDENT);
               // We have to add it to the stack before the queue in case if it
               // got removed by another miss
               DequeNode<LIRSNode<K, V>> stackNode = new DequeNode<>(lirsNode);
               lirsNode.setStackNode(stackNode);
               stack.linkLast(stackNode);
               DequeNode<LIRSNode<K, V>> queueNode = new DequeNode<>(lirsNode);
               lirsNode.setQueueNode(queueNode);
               queue.linkLast(queueNode);
            } else {
               // If this was true it means this node was not created by this miss
               // (essentially means the value was changed to HIR_NONRESIDENT or is being
               // EVICTED soon
               if (skipIncrement) {
                  switch (recency) {
                  case HIR_NONRESIDENT:
                     // This is the 3.3 case 3 (a) example
                     e.val = value;
                     // We don't add a value to the map here as the map implementation
                     // will handle this for us afterwards

                     // If it was added to LIR due to size don't do anymore
                     if (addToLIRIfNotFullHot(lirsNode, true)) {
                        return;
                     }

                     // This is the (a) example
                     promoteHIRToLIR(lirsNode);
                     pruneLIR = true;
                     // Since we are adding back in a value we set it to increment
                     skipIncrement = false;
                     break;
                  case EVICTING:
                     e.val = value;
                     // We don't add a value to the map here as the map implementation
                     // will handle this for us afterwards
                     
                     // In the case of eviction we add the node back as if it was a HIR_RESIDENT
                     // except we also have to evict an old HIR to make room
                     evictHIR = true;
                     lirsNode.setState(Recency.HIR_RESIDENT);
                     // We have to add it back now
                     DequeNode<LIRSNode<K, V>> stackNode = new DequeNode<>(lirsNode);
                     lirsNode.setStackNode(stackNode);
                     stack.linkLast(stackNode);
                     DequeNode<LIRSNode<K, V>> queueNode = new DequeNode<>(lirsNode);
                     lirsNode.setQueueNode(queueNode);
                     queue.linkLast(queueNode);
                     break;
                  case REMOVED:
                  case EVICTED:
                     // It cannot turn into a resident since it would require a put to
                     // update the value to non null, which would require holding the lock
                     // Both Removed and Evicted hold the lock during the entire duration
                     // so neither state should be possible as they will remove node
                     throw new IllegalStateException("Cannot have a miss on a key and then "
                           + "get a node in " + recency);
                  case HIR_RESIDENT:
                  case LIR_RESIDENT:
                     // Ignore any entry in this state as it is already updated
                     break;
                  }
               }
            }
         }

         if (pruneLIR) {
            hotDemotion.incrementAndGet();
         }

         // Note only 1 of these can be true
         if (!skipIncrement || evictHIR) {
            // The size is checked in the findIfEntriesNeedEvicting
            incrementSizeEviction(currentSize, 1, 0);
         }
      }

      @SuppressWarnings("unchecked")
      void demoteLowestLIR() {
         boolean pruned = false;
         while (!pruned) {
            // Now we prune to make room for our promoted node
            Object[] LIRDetails = pruneIncludingLIR();
            if (LIRDetails == null) {
               // There was nothing to prune!
               return;
            } else {
               DequeNode<LIRSNode<K, V>> removedDequeNode = (DequeNode<LIRSNode<K, V>>) LIRDetails[0];
               LIRSNode<K, V> removedLIR = (LIRSNode<K, V>) LIRDetails[1];
               synchronized (removedLIR) {
                  if (removedDequeNode != removedLIR.stackNode) {
                     continue;
                  }
                  // If the node was removed concurrently removed we ignore it and get the
                  // next one still
                  if (removedLIR.state != Recency.REMOVED){
                     // If the stack node is still the one we removed, then we can continue
                     // with demotion.  If not then we had a concurrent hit which resurrected
                     // the LIR so we pick the next one to evict

                     // We demote the LIR_RESIDENT to HIR_RESIDENT in the queue (not in stack)
                     removedLIR.setState(Recency.HIR_RESIDENT);
                     removedLIR.setStackNode(null);
                     DequeNode<LIRSNode<K, V>> queueNode = new DequeNode<>(removedLIR);
                     removedLIR.setQueueNode(queueNode);
                     queue.linkLast(queueNode);
                     pruned = true;
                  }
               }
            }
         }
      }

      /**
       * Prunes blocks in the bottom of the stack until a HOT block is removed.
       * If pruned blocks were resident, then they remain in the queue; non-resident blocks (if any)
       * are dropped
       * @return Returns an array storing the removed LIR details.  The first element is 
       *         the DequeNode that was removed from the stack deque - this
       *         is helpful to determine if this entry was update concurrently (because
       *         it will have a new stack deque pointer if it was).  The second element is
       *         the actual Node that this is tied to value wise
       */
      @SuppressWarnings("unchecked")
      Object[] pruneIncludingLIR() {
         // See section 3.3:
         // "We define an operation called "stack pruning" on the LIRS
         // stack S, which removes the HIR blocks in the bottom of
         // the stack until an LIR block sits in the stack bottom. This
         // operation serves for two purposes: (1) We ensure the block in
         // the bottom of the stack always belongs to the LIR block set.
         // (2) After the LIR block in the bottom is removed, those HIR
         // blocks contiguously located above it will not have chances to
         // change their status from HIR to LIR, because their recencies
         // are larger than the new maximum recency of LIR blocks."
         /**
          * Note that our implementation is done lazily and we don't prune HIR blocks
          * until we know we are removing a LIR block.  The reason for this is that
          * we can't do a head CAS and only can poll from them
          * WARNING: This could cause a HIR block that should have been pruned to be
          * promoted to LIR if it is accesssed before another LIR block is promoted.
          * Unfortunately there is not an easier way to do this without adding additional
          * contention.  Need to figure out if the contention would be high or not
          */
         LIRSNode<K, V> removedLIR = null;
         Object[] nodeDetails = new Object[2];
         while (true) {
            if (!stack.pollFirstNode(nodeDetails)) {
               long hot;
               // If hot size is less than 0 that means we had a concurrent removal of
               // the last contents in the cache, so we need to make sure to not to loop
               // waiting for a value
               while ((hot = hotSize.get()) < 0) {
                  if (hotSize.compareAndSet(hot, hot + 1)) {
                     return null;
                  }
               }
               continue;
            }
            DequeNode<LIRSNode<K, V>> removedStackNode = (DequeNode<LIRSNode<K, V>>) nodeDetails[0];
            removedLIR = (LIRSNode<K, V>) nodeDetails[1];
            synchronized (removedLIR) {
               if (removedStackNode != removedLIR.stackNode) {
                  continue;
               }
               switch (removedLIR.state) {
               case LIR_RESIDENT:
                  // Note we don't null out the stack because the caller once again
                  // does a check to make sure the stack pointer hasn't changed
                  return nodeDetails;
               case HIR_NONRESIDENT:
                  // Non resident was already evicted and now it is no longer in the
                  // queue or the stack so it is effectively gone - however we want to
                  // remove the now null node
                  removedLIR.setState(Recency.EVICTING);
                  Collection<LIRSNode<K, V>> nodesToEvict = nodesToEvictTL.get();
                  if (nodesToEvict == null) {
                     nodesToEvict = new ArrayList<>();
                     nodesToEvictTL.set(nodesToEvict);
                  }
                  nodesToEvict.add(removedLIR);
               case HIR_RESIDENT:
                  // Leave it in the queue if it was a resident
                  removedLIR.setStackNode(null);
                  break;
               case REMOVED:
               case EVICTING:
               case EVICTED:
                  // We ignore this value if it it was evicted/removed elsewhere as it is
                  // no longer LIRS
                  break;
               }
            }
         }
      }

      /**
       * The node must be locked before calling this method
       * @param lirsNode
       */
      void promoteHIRToLIR(LIRSNode<K, V> lirsNode) {
         // This block first unlinks the node from both the stack and queue before
         // repositioning it
         {
            DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
            // Stack node could be null if this node was pruned in demotion concurrently
            if (stackNode != null) {
               LIRSNode<K, V> item = stackNode.item;
               if (item != null && stackNode.casItem(item, null)) {
                  stack.unlink(stackNode);
               }
               lirsNode.setStackNode(null);
            }
            
            // Also unlink from queue node if it was set
            DequeNode<LIRSNode<K, V>> queueNode = lirsNode.queueNode;
            if (queueNode != null) {
               LIRSNode<K, V> item = queueNode.item;
               if (item != null && queueNode.casItem(item, null)) {
                  queue.unlink(queueNode);
               }
               lirsNode.setQueueNode(null);
            }
         }

         // Promoting the node to LIR and add to the stack
         lirsNode.setState(Recency.LIR_RESIDENT);
         DequeNode<LIRSNode<K, V>> stackNode = new DequeNode<>(lirsNode);
         lirsNode.setStackNode(stackNode);
         stack.linkLast(stackNode);
      }

      @Override
      public void onEntryHitRead(Node<K,V> e, V value) {
         boolean reAttempt = false;
         LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
         synchronized (lirsNode) {
            Recency recency = lirsNode.state;
            // If the recency is non resident or evicting that means
            // we may have to do a write to the value most likely, so we retry this
            // with the table lock so we can properly do the update - NOTE that the
            // recency can change outside of this lock
            if (recency == Recency.HIR_NONRESIDENT || 
                  recency == Recency.EVICTING) {
               reAttempt = true;
            } else {
               onEntryHitWrite(e, value);
            }
         }
         if (reAttempt) {
            int hash = spread(map.keyEq.hashCode(lirsNode.getKey())); // EQUIVALENCE_MOD
            for (Node<K,V>[] tab = map.table;;) {
               Node<K,V> f; int n, i;
               if (tab == null || (n = tab.length) == 0 ||
                     (f = tabAt(tab, i = (n - 1) & hash)) == null)
                  break;
               else if (f.hash == MOVED)
                  tab = map.helpTransfer(tab, f);
               else {
                  synchronized (f) {
                     if (tabAt(tab, i) == f) {
                        synchronized (lirsNode) {
                           onEntryHitWrite(e, value);
                        }
                     }
                  }
                  break;
               }
            }
         }
      }

      @Override
      public void onEntryHitWrite(Node<K, V> e, V value) {
         boolean demoteLIR = false;
         boolean evictHIR = false;
         LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
         synchronized (lirsNode) {
            // Section 3.3
            //
            Recency recency = lirsNode.state;
            // If the state is still null that means it was added and we got in
            // before the onEntryMiss was fired, so that means this is automatically
            // a LIR resident
            if (recency == null) {
               // If it was added to LIR don't need anymore work
               if (addToLIRIfNotFullHot(lirsNode, false)) {
                  return;
               }
               recency = Recency.LIR_RESIDENT;
               lirsNode.setState(recency);
               // If we are doing a promotion of HIR to LIR we need to do pruning as well
               // Remember if recency was null that means we just had a concurrent
               // onEntryMiss but it hasn't been processed yet, which means it would
               // been a HIR resident
               demoteLIR = true;
            }

            switch (recency) {
            case LIR_RESIDENT:
               // case 1
               //
               // Note that if we had a concurrent pruning targeting this node, getting
               // a hit takes precedence
               DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
               // This will be null if we got in before onEntryMiss
               if (stackNode != null) {
                  LIRSNode<K, V> item = stackNode.item;
                  if (item != null && stackNode.casItem(item, null)) {
                     stack.unlink(stackNode);
                  }
               }
               // Now that we have it removed promote it to the top
               DequeNode<LIRSNode<K, V>> newStackNode = new DequeNode<>(lirsNode);
               lirsNode.setStackNode(newStackNode);
               stack.linkLast(newStackNode);
               break;
            case HIR_NONRESIDENT:
               if (e.val == NULL_VALUE) {
                  // We essentially added the value back in so we have to set the value
                  // and increment the count
                  e.val = value;
                  map.addCount(1, -1);
               }
               // Non resident can happen if we have a hit and then a concurrent HIR pruning
               // caused our node to be non resident.
               if (addToLIRIfNotFullHot(lirsNode, true)) {
                  return;
               }
               promoteHIRToLIR(lirsNode);
               // The only way this is possible is if we had a concurrent eviction
               // of the key right after we saw it existed but before we could
               // act on it.  Since we revived it to LIR we also have to set it's
               // value to what it should be
               evictHIR = true;
               demoteLIR = true;
               break;
            case EVICTED:
               // We can't reliably do a put here without having a point where the object
               // was possibly seen as null so we leave it as null to be more consistent
               break;
            case EVICTING:
               // In the case of eviction we add the node back as if it was a HIR_RESIDENT
               // except we also have to evict an old HIR to make room
               // Note the stackNode is assumed to be null in this case
               evictHIR = true;
               lirsNode.setState(Recency.HIR_RESIDENT);
               // It is possible the node trying to be evicted was a NON resident before
               // In that case we set the value and increment the count in the map
               if (e.val == NULL_VALUE) {
                  e.val = value;
                  map.addCount(1, -1);
               }
               // Note this falls through, since we saved the evicting entry it is like
               // we now have a HIR resident
            case HIR_RESIDENT:
               // case 2
               if (lirsNode.stackNode != null) {
                  // This is the (a) example
                  //
                  // In the case it was in the stack we promote it
                  promoteHIRToLIR(lirsNode);
                  // Need to demote a LIR to make room
                  demoteLIR = true;
               } else {
                  // This is the (b) example
                  //
                  // In the case it wasn't in the stack but was in the queue, we
                  // add it again to the stack and bump it up to the top of the queue
                  if (lirsNode.queueNode != null) {
                     LIRSNode<K, V> item = lirsNode.queueNode.item;
                     if (item != null && lirsNode.queueNode.casItem(item, null)) {
                        queue.unlink(lirsNode.queueNode);
                     }
                  }

                  newStackNode = new DequeNode<>(lirsNode);
                  lirsNode.setStackNode(newStackNode);
                  stack.linkLast(newStackNode);
                  DequeNode<LIRSNode<K, V>> newQueueNode = new DequeNode<>(lirsNode);
                  lirsNode.setQueueNode(newQueueNode);
                  queue.linkLast(newQueueNode);
               }
               break;
            case REMOVED:
               // If the entry was removed we ignore the hit.
               break;
            }
         }
         if (demoteLIR) {
            hotDemotion.incrementAndGet();
         }
         if (evictHIR) {
            // The size is checked in the findIfEntriesNeedEvicting
            incrementSizeEviction(currentSize, 1, 0);
         }
      }

      @Override
      public void onEntryRemove(Node<K, V> e) {
         LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
         synchronized (lirsNode) {
            switch (lirsNode.state) {
            case LIR_RESIDENT:
               hotSize.decrementAndGet();
            case HIR_RESIDENT:
               incrementSizeEviction(currentSize, -1, 0);
            case HIR_NONRESIDENT:
            case EVICTING:
               // In the case of eviction/non resident we already subtracted the value
               // And we don't want to remove twice
               lirsNode.setState(Recency.REMOVED);
               break;
            case REMOVED:
            case EVICTED:
               // This shouldn't be possible
            }

            DequeNode<LIRSNode<K, V>> queueNode = lirsNode.queueNode;
            if (queueNode != null) {
               LIRSNode<K, V> item = queueNode.item;
               if (item != null && queueNode.casItem(item, null)) {
                  queue.unlink(queueNode);
               }
               lirsNode.setQueueNode(null);
            }
            lirsNode.setQueueNode(null);
            DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
            if (stackNode != null) {
               LIRSNode<K, V> item = stackNode.item;
               if (item != null && stackNode.casItem(item, null)) {
                  stack.unlink(stackNode);
               }
               lirsNode.setStackNode(null);
            }
         }
      }

      @Override
      public Collection<Node<K, V>> findIfEntriesNeedEvicting() {
         long hotDemotions;
         while ((hotDemotions = hotDemotion.get()) > 0) {
            if (hotDemotion.compareAndSet(hotDemotions, 0)) {
               break;
            }
         }
         for (long i = 0; i < hotDemotions; ++i) {
            demoteLowestLIR();
         }
         int evictCount;
         while (true) {
            SizeAndEvicting sizeEvict = currentSize.get();
            long size = sizeEvict.size;
            long evict = sizeEvict.evicting;
            long longEvictCount = (size - evict - maximumSize);
            if (longEvictCount > 0) {
               evictCount = (int) longEvictCount & 0x7fffffff;
               if (currentSize.compareAndSet(sizeEvict,
                     new SizeAndEvicting(size, evict + evictCount))) {
                  break;
               }
            } else {
               evictCount = 0;
               break;
            }
         }
         // If this is non null it is also non empty
         Collection<LIRSNode<K, V>> tlEvicted = nodesToEvictTL.get();
         if (tlEvicted == null) {
            tlEvicted = InfinispanCollections.emptyList();
         } else {
            nodesToEvictTL.remove();
         }
         if (evictCount != 0 || !tlEvicted.isEmpty()) {
            @SuppressWarnings("unchecked")
            LIRSNode<K, V>[] queueContents = new LIRSNode[evictCount + tlEvicted.size()];
            Iterator<LIRSNode<K, V>> tlIterator = tlEvicted.iterator();
            int offset = 0;
            while (tlIterator.hasNext()) {
               queueContents[evictCount + offset] = tlIterator.next();
               offset++;
            }
            int evictedValues = evictCount;
            int decEvict = evictCount;
            Object[] hirDetails = new Object[2];
            for (int i = 0; i < evictCount; ++i) {
               boolean foundNode = false;
               while (!foundNode) {
                  if (!queue.pollFirstNode(hirDetails)) {
                     SizeAndEvicting sizeEvict = currentSize.get();
                     // If the size was changed behind our back by a remove we
                     // need to detect that
                     if (sizeEvict.size - sizeEvict.evicting < maximumSize) {
                        SizeAndEvicting newSizeEvict = new SizeAndEvicting(sizeEvict.size,
                              sizeEvict.evicting - 1);
                        if (currentSize.compareAndSet(sizeEvict, newSizeEvict)) {
                           evictedValues--;
                           decEvict--;
                           break;
                        }
                     }
                     // This could be valid in the case when an entry is promoted to LIR and
                     // hasn't yet moved the demoted LIR to HIR
                     // We loop back around to get this - this could spin loop if it takes
                     // the other thread long to add the HIR element back
                     LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                     continue;
                  }
                  DequeNode<LIRSNode<K, V>> removedDequeNode = (DequeNode<LIRSNode<K, V>>) hirDetails[0];
                  LIRSNode<K, V> removedHIR = (LIRSNode<K, V>) hirDetails[1];
                  synchronized (removedHIR) {
                     if (removedHIR.queueNode != removedDequeNode) {
                        continue;
                     }
                     // Since we removed the queue node we need to null it out
                     removedHIR.setQueueNode(null);
                     switch (removedHIR.state) {
                     case HIR_RESIDENT:
                        // If it is in the stack we set it to HIR non resident
                        if (removedHIR.stackNode != null) {
                           removedHIR.setState(Recency.HIR_NONRESIDENT);
                           // We can't set the value to NULL_VALUE until we hold the
                           // lock below
                           queueContents[i] = removedHIR;
                        } else {
                           removedHIR.setState(Recency.EVICTING);
                           // It wasn't part of the stack so we have to remove it completely
                           // Note we don't null out the queue or stack nodes on the LIRSNode
                           // since we aren't reusing it
                           queueContents[i] = removedHIR;
                        }
                        foundNode = true;
                        break;
                     case REMOVED:
                        // If it was removed then it wasn't evicted so we don't decrement
                        // size in that case
                        evictedValues--;
                        foundNode = true;
                        break;
                     case LIR_RESIDENT:
                     case EVICTING:
                     case EVICTED:
                     case HIR_NONRESIDENT:
                        break;
                     }
                  }
               }
            }
            incrementSizeEviction(currentSize, -evictedValues, -decEvict);
            Collection<Node<K, V>> removedNodes = new ArrayList<>(queueContents.length);
            for (int j = 0; j < queueContents.length; ++j) {
               LIRSNode<K, V> evict = queueContents[j];
               // This can be null if the entry was removed when we tried to evict
               // or if we found a subsequent value in the same table
               if (evict == null) {
                  continue;
               }
               // If it is still evicting we lock the segment for the key and then
               // finally evict it
               // The locking of the outer segment is required if the entry is hit with
               // an update at the same time
               if (evict.state == Recency.EVICTING || 
                     evict.state == Recency.HIR_NONRESIDENT) {
                  // Most of the following is copied from putVal method of CHMV8
                  // This is so we can get the owning node so we can synchronize
                  // to make sure that no additional writes occur for the evicting
                  // value because we don't want them to be lost when we do the actual
                  // eviction
                  int hash = spread(map.keyEq.hashCode(evict.getKey())); // EQUIVALENCE_MOD
                  for (Node<K,V>[] tab = map.table;;) {
                     Node<K,V> f; int n, i;
                     if (tab == null || (n = tab.length) == 0 ||
                           (f = tabAt(tab, i = (n - 1) & hash)) == null)
                        break;
                     else if (f.hash == MOVED)
                        tab = map.helpTransfer(tab, f);
                     else {
                        synchronized (f) {
                           if (tabAt(tab, i) == f) {
                              synchronized (evict) {
                                 if (evict.state == Recency.EVICTING) {
                                    evict.setState(Recency.EVICTED);
                                    V prevValue = map.replaceNode(evict.getKey(), null, null, true);
                                    removedNodes.add(new Node<>(-1, null, evict.getKey(),
                                          prevValue, null));
                                 } else if (evict.state == Recency.HIR_NONRESIDENT) {
                                    Node<K, V> node = f.find(hash, evict.getKey());
                                    V prevValue = node.val;
                                    if (prevValue != NULL_VALUE) {
                                       node.val = (V) NULL_VALUE;
                                       map.addCount(-1, -1);
                                       Node<K, V> nonResidentNode = new Node<K, V>(-1, null, evict.getKey(),
                                             prevValue, null);
                                       removedNodes.add(nonResidentNode);
                                       map.notifyListenerOfRemoval(nonResidentNode, true);
                                    }
                                 }
                              }
                              break;
                           }
                        }
                     }
                  }
               }
            }
            return removedNodes;
         }
         return InfinispanCollections.emptySet();
      }

      @Override
      public void onResize(long oldSize, long newSize) {
         // Do nothing
      }

   }

   public enum Eviction {
      NONE {
         @Override
         public <K, V> EvictionPolicy<K, V> make(
               BoundedEquivalentConcurrentHashMapV8<K, V> map, 
               EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            return new NullEvictionPolicy<K, V>(map.nodeEq);
         }
      },
      LRU {
         @Override
         public <K, V> EvictionPolicy<K, V> make(
               BoundedEquivalentConcurrentHashMapV8<K, V> map, 
               EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            if (sizeCalculator == null) {
               return new LRUEvictionPolicy<K, V>(map, capacity,
                     SingleEntrySizeCalculator.SINGLETON, false);
            } else {
               return new LRUEvictionPolicy<K, V>(map, capacity,
                     new NodeSizeCalculatorWrapper<K, V>(sizeCalculator), true);
            }
            
         }
      },
      LIRS {
         @Override
         public <K, V> EvictionPolicy<K, V> make(BoundedEquivalentConcurrentHashMapV8<K, V> map, 
               EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            if (sizeCalculator != null) {
               throw new IllegalArgumentException("LIRS does not support a size calculator!");
            }
            return new LIRSEvictionPolicy<K, V>(map, capacity);
         }
      };

      abstract <K, V> EvictionPolicy<K, V> make(
            BoundedEquivalentConcurrentHashMapV8<K, V> map, EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity);
   }
   // END EVICTION STUFF

   /**
    * An object for traversing and partitioning elements of a source.
    * This interface provides a subset of the functionality of JDK8
    * java.util.Spliterator.
    */
   public static interface ConcurrentHashMapSpliterator<T> {
      /**
       * If possible, returns a new spliterator covering
       * approximately one half of the elements, which will not be
       * covered by this spliterator. Returns null if cannot be
       * split.
       */
      ConcurrentHashMapSpliterator<T> trySplit();
      /**
       * Returns an estimate of the number of elements covered by
       * this Spliterator.
       */
      long estimateSize();

      /** Applies the action to each untraversed element */
      void forEachRemaining(Consumer<? super T> action);
      /** If an element remains, applies the action and returns true. */
      boolean tryAdvance(Consumer<? super T> action);
   }

    /*
     * Overview:
     *
     * The primary design goal of this hash table is to maintain
     * concurrent readability (typically method get(), but also
     * iterators and related methods) while minimizing update
     * contention. Secondary goals are to keep space consumption about
     * the same or better than java.util.HashMap, and to support high
     * initial insertion rates on an empty table by many threads.
     *
     * This map usually acts as a binned (bucketed) hash table.  Each
     * key-value mapping is held in a Node.  Most nodes are instances
     * of the basic Node class with hash, key, value, and next
     * fields. However, various subclasses exist: TreeNodes are
     * arranged in balanced trees, not lists.  TreeBins hold the roots
     * of sets of TreeNodes. ForwardingNodes are placed at the heads
     * of bins during resizing. ReservationNodes are used as
     * placeholders while establishing values in computeIfAbsent and
     * related methods.  The types TreeBin, ForwardingNode, and
     * ReservationNode do not hold normal user keys, values, or
     * hashes, and are readily distinguishable during search etc
     * because they have negative hash fields and null key and value
     * fields. (These special nodes are either uncommon or transient,
     * so the impact of carrying around some unused fields is
     * insignificant.)
     *
     * The table is lazily initialized to a power-of-two size upon the
     * first insertion.  Each bin in the table normally contains a
     * list of Nodes (most often, the list has only zero or one Node).
     * Table accesses require volatile/atomic reads, writes, and
     * CASes.  Because there is no other way to arrange this without
     * adding further indirections, we use intrinsics
     * (sun.misc.Unsafe) operations.
     *
     * We use the top (sign) bit of Node hash fields for control
     * purposes -- it is available anyway because of addressing
     * constraints.  Nodes with negative hash fields are specially
     * handled or ignored in map methods.
     *
     * Insertion (via put or its variants) of the first node in an
     * empty bin is performed by just CASing it to the bin.  This is
     * by far the most common case for put operations under most
     * key/hash distributions.  Other update operations (insert,
     * delete, and replace) require locks.  We do not want to waste
     * the space required to associate a distinct lock object with
     * each bin, so instead use the first node of a bin list itself as
     * a lock. Locking support for these locks relies on builtin
     * "synchronized" monitors.
     *
     * Using the first node of a list as a lock does not by itself
     * suffice though: When a node is locked, any update must first
     * validate that it is still the first node after locking it, and
     * retry if not. Because new nodes are always appended to lists,
     * once a node is first in a bin, it remains first until deleted
     * or the bin becomes invalidated (upon resizing).
     *
     * The main disadvantage of per-bin locks is that other update
     * operations on other nodes in a bin list protected by the same
     * lock can stall, for example when user equals() or mapping
     * functions take a long time.  However, statistically, under
     * random hash codes, this is not a common problem.  Ideally, the
     * frequency of nodes in bins follows a Poisson distribution
     * (http://en.wikipedia.org/wiki/Poisson_distribution) with a
     * parameter of about 0.5 on average, given the resizing threshold
     * of 0.75, although with a large variance because of resizing
     * granularity. Ignoring variance, the expected occurrences of
     * list size k are (exp(-0.5) * pow(0.5, k) / factorial(k)). The
     * first values are:
     *
     * 0:    0.60653066
     * 1:    0.30326533
     * 2:    0.07581633
     * 3:    0.01263606
     * 4:    0.00157952
     * 5:    0.00015795
     * 6:    0.00001316
     * 7:    0.00000094
     * 8:    0.00000006
     * more: less than 1 in ten million
     *
     * Lock contention probability for two threads accessing distinct
     * elements is roughly 1 / (8 * #elements) under random hashes.
     *
     * Actual hash code distributions encountered in practice
     * sometimes deviate significantly from uniform randomness.  This
     * includes the case when N > (1<<30), so some keys MUST collide.
     * Similarly for dumb or hostile usages in which multiple keys are
     * designed to have identical hash codes or ones that differs only
     * in masked-out high bits. So we use a secondary strategy that
     * applies when the number of nodes in a bin exceeds a
     * threshold. These TreeBins use a balanced tree to hold nodes (a
     * specialized form of red-black trees), bounding search time to
     * O(log N).  Each search step in a TreeBin is at least twice as
     * slow as in a regular list, but given that N cannot exceed
     * (1<<64) (before running out of addresses) this bounds search
     * steps, lock hold times, etc, to reasonable constants (roughly
     * 100 nodes inspected per operation worst case) so long as keys
     * are Comparable (which is very common -- String, Long, etc).
     * TreeBin nodes (TreeNodes) also maintain the same "next"
     * traversal pointers as regular nodes, so can be traversed in
     * iterators in the same way.
     *
     * The table is resized when occupancy exceeds a percentage
     * threshold (nominally, 0.75, but see below).  Any thread
     * noticing an overfull bin may assist in resizing after the
     * initiating thread allocates and sets up the replacement array.
     * However, rather than stalling, these other threads may proceed
     * with insertions etc.  The use of TreeBins shields us from the
     * worst case effects of overfilling while resizes are in
     * progress.  Resizing proceeds by transferring bins, one by one,
     * from the table to the next table. However, threads claim small
     * blocks of indices to transfer (via field transferIndex) before
     * doing so, reducing contention.  A generation stamp in field
     * sizeCtl ensures that resizings do not overlap. Because we are
     * using power-of-two expansion, the elements from each bin must
     * either stay at same index, or move with a power of two
     * offset. We eliminate unnecessary node creation by catching
     * cases where old nodes can be reused because their next fields
     * won't change.  On average, only about one-sixth of them need
     * cloning when a table doubles. The nodes they replace will be
     * garbage collectable as soon as they are no longer referenced by
     * any reader thread that may be in the midst of concurrently
     * traversing table.  Upon transfer, the old table bin contains
     * only a special forwarding node (with hash field "MOVED") that
     * contains the next table as its key. On encountering a
     * forwarding node, access and update operations restart, using
     * the new table.
     *
     * Each bin transfer requires its bin lock, which can stall
     * waiting for locks while resizing. However, because other
     * threads can join in and help resize rather than contend for
     * locks, average aggregate waits become shorter as resizing
     * progresses.  The transfer operation must also ensure that all
     * accessible bins in both the old and new table are usable by any
     * traversal.  This is arranged in part by proceeding from the
     * last bin (table.length - 1) up towards the first.  Upon seeing
     * a forwarding node, traversals (see class Traverser) arrange to
     * move to the new table without revisiting nodes.  To ensure that
     * no intervening nodes are skipped even when moved out of order,
     * a stack (see class TableStack) is created on first encounter of
     * a forwarding node during a traversal, to maintain its place if
     * later processing the current table. The need for these
     * save/restore mechanics is relatively rare, but when one
     * forwarding node is encountered, typically many more will be.
     * So Traversers use a simple caching scheme to avoid creating so
     * many new TableStack nodes. (Thanks to Peter Levart for
     * suggesting use of a stack here.)
     *
     * The traversal scheme also applies to partial traversals of
     * ranges of bins (via an alternate Traverser constructor)
     * to support partitioned aggregate operations.  Also, read-only
     * operations give up if ever forwarded to a null table, which
     * provides support for shutdown-style clearing, which is also not
     * currently implemented.
     *
     * Lazy table initialization minimizes footprint until first use,
     * and also avoids resizings when the first operation is from a
     * putAll, constructor with map argument, or deserialization.
     * These cases attempt to override the initial capacity settings,
     * but harmlessly fail to take effect in cases of races.
     *
     * The element count is maintained using a specialization of
     * LongAdder. We need to incorporate a specialization rather than
     * just use a LongAdder in order to access implicit
     * contention-sensing that leads to creation of multiple
     * CounterCells.  The counter mechanics avoid contention on
     * updates but can encounter cache thrashing if read too
     * frequently during concurrent access. To avoid reading so often,
     * resizing under contention is attempted only upon adding to a
     * bin already holding two or more nodes. Under uniform hash
     * distributions, the probability of this occurring at threshold
     * is around 13%, meaning that only about 1 in 8 puts check
     * threshold (and after resizing, many fewer do so).
     *
     * TreeBins use a special form of comparison for search and
     * related operations (which is the main reason we cannot use
     * existing collections such as TreeMaps). TreeBins contain
     * Comparable elements, but may contain others, as well as
     * elements that are Comparable but not necessarily Comparable for
     * the same T, so we cannot invoke compareTo among them. To handle
     * this, the tree is ordered primarily by hash value, then by
     * Comparable.compareTo order if applicable.  On lookup at a node,
     * if elements are not comparable or compare as 0 then both left
     * and right children may need to be searched in the case of tied
     * hash values. (This corresponds to the full list search that
     * would be necessary if all elements were non-Comparable and had
     * tied hashes.) On insertion, to keep a total ordering (or as
     * close as is required here) across rebalancings, we compare
     * classes and identityHashCodes as tie-breakers. The red-black
     * balancing code is updated from pre-jdk-collections
     * (http://gee.cs.oswego.edu/dl/classes/collections/RBCell.java)
     * based in turn on Cormen, Leiserson, and Rivest "Introduction to
     * Algorithms" (CLR).
     *
     * TreeBins also require an additional locking mechanism.  While
     * list traversal is always possible by readers even during
     * updates, tree traversal is not, mainly because of tree-rotations
     * that may change the root node and/or its linkages.  TreeBins
     * include a simple read-write lock mechanism parasitic on the
     * main bin-synchronization strategy: Structural adjustments
     * associated with an insertion or removal are already bin-locked
     * (and so cannot conflict with other writers) but must wait for
     * ongoing readers to finish. Since there can be only one such
     * waiter, we use a simple scheme using a single "waiter" field to
     * block writers.  However, readers need never block.  If the root
     * lock is held, they proceed along the slow traversal path (via
     * next-pointers) until the lock becomes available or the list is
     * exhausted, whichever comes first. These cases are not fast, but
     * maximize aggregate expected throughput.
     *
     * Maintaining API and serialization compatibility with previous
     * versions of this class introduces several oddities. Mainly: We
     * leave untouched but unused constructor arguments refering to
     * concurrencyLevel. We accept a loadFactor constructor argument,
     * but apply it only to initial table capacity (which is the only
     * time that we can guarantee to honor it.) We also declare an
     * unused "Segment" class that is instantiated in minimal form
     * only when serializing.
     *
     * Also, solely for compatibility with previous versions of this
     * class, it extends AbstractMap, even though all of its methods
     * are overridden, so it is just useless baggage.
     *
     * This file is organized to make things a little easier to follow
     * while reading than they might otherwise: First the main static
     * declarations and utilities, then fields, then main public
     * methods (with a few factorings of multiple public methods into
     * internal ones), then sizing methods, trees, traversers, and
     * bulk operations.
     */

    /* ---------------- Constants -------------- */

   /**
    * The largest possible table capacity.  This value must be
    * exactly 1<<30 to stay within Java array allocation and indexing
    * bounds for power of two table sizes, and is further required
    * because the top two bits of 32bit hash fields are used for
    * control purposes.
    */
   private static final int MAXIMUM_CAPACITY = 1 << 30;

   /**
    * The default initial table capacity.  Must be a power of 2
    * (i.e., at least 1) and at most MAXIMUM_CAPACITY.
    */
   private static final int DEFAULT_CAPACITY = 16;

   /**
    * The largest possible (non-power of two) array size.
    * Needed by toArray and related methods.
    */
   static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

   /**
    * The default concurrency level for this table. Unused but
    * defined for compatibility with previous versions of this class.
    */
   private static final int DEFAULT_CONCURRENCY_LEVEL = 16;

   /**
    * The load factor for this table. Overrides of this value in
    * constructors affect only the initial table capacity.  The
    * actual floating point value isn't normally used -- it is
    * simpler to use expressions such as {@code n - (n >>> 2)} for
    * the associated resizing threshold.
    */
   private static final float LOAD_FACTOR = 0.75f;

   /**
    * The bin count threshold for using a tree rather than list for a
    * bin.  Bins are converted to trees when adding an element to a
    * bin with at least this many nodes. The value must be greater
    * than 2, and should be at least 8 to mesh with assumptions in
    * tree removal about conversion back to plain bins upon
    * shrinkage.
    */
   static final int TREEIFY_THRESHOLD = 8;

   /**
    * The bin count threshold for untreeifying a (split) bin during a
    * resize operation. Should be less than TREEIFY_THRESHOLD, and at
    * most 6 to mesh with shrinkage detection under removal.
    */
   static final int UNTREEIFY_THRESHOLD = 6;

   /**
    * The smallest table capacity for which bins may be treeified.
    * (Otherwise the table is resized if too many nodes in a bin.)
    * The value should be at least 4 * TREEIFY_THRESHOLD to avoid
    * conflicts between resizing and treeification thresholds.
    */
   static final int MIN_TREEIFY_CAPACITY = 64;

   /**
    * Minimum number of rebinnings per transfer step. Ranges are
    * subdivided to allow multiple resizer threads.  This value
    * serves as a lower bound to avoid resizers encountering
    * excessive memory contention.  The value should be at least
    * DEFAULT_CAPACITY.
    */
   private static final int MIN_TRANSFER_STRIDE = 16;

   /**
    * The number of bits used for generation stamp in sizeCtl.
    * Must be at least 6 for 32bit arrays.
    */
   private static int RESIZE_STAMP_BITS = 16;

   /**
    * The maximum number of threads that can help resize.
    * Must fit in 32 - RESIZE_STAMP_BITS bits.
    */
   private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

   /**
    * The bit shift for recording size stamp in sizeCtl.
    */
   private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;

   /*
     * Encodings for Node hash fields. See above for explanation.
     */
   static final int MOVED     = -1; // hash for forwarding nodes
   static final int TREEBIN   = -2; // hash for roots of trees
   static final int RESERVED  = -3; // hash for transient reservations
   static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

   /** Number of CPUS, to place bounds on some sizings */
   static final int NCPU = Runtime.getRuntime().availableProcessors();

   /** For serialization compatibility. */
   private static final ObjectStreamField[] serialPersistentFields = {
         new ObjectStreamField("segments", Segment[].class),
         new ObjectStreamField("segmentMask", Integer.TYPE),
         new ObjectStreamField("segmentShift", Integer.TYPE)
   };

    /* ---------------- Nodes -------------- */

   static class NodeEquivalence<K,V> {
      final Equivalence<? super K> keyEq;
      final Equivalence<? super V> valueEq;

      NodeEquivalence(Equivalence<? super K> keyEq, Equivalence<? super V> valueEq) {
         this.keyEq = keyEq;
         this.valueEq = valueEq;
      }
   }

   /**
    * Key-value entry.  This class is never exported out as a
    * user-mutable Map.Entry (i.e., one supporting setValue; see
    * MapEntry below), but can be used for read-only traversals used
    * in bulk tasks.  Subclasses of Node with a negative hash field
    * are special, and contain null keys and values (but are never
    * exported).  Otherwise, keys and vals are never null.
    */
   static class Node<K,V> implements Map.Entry<K,V> {
      final int hash;
      final K key;
      final NodeEquivalence<K, V> nodeEq; // EQUIVALENCE_MOD
      volatile V val;
      volatile Node<K,V> next;
      volatile EvictionEntry<K, V> eviction;

      Node(int hash, NodeEquivalence<K, V> nodeEq, K key, 
            V val, Node<K,V> next) { // EQUIVALENCE_MOD
         this.hash = hash;
         this.key = key;
         this.val = val;
         this.next = next;
         this.nodeEq = nodeEq; // EQUIVALENCE_MOD
      }

      public final int hashCode(K key, V value) {
         return nodeEq.keyEq.hashCode(key) ^ nodeEq.valueEq.hashCode(value);
      }

      public final K getKey()       { return key; }
      public final V getValue()     { return val; }
      public final int hashCode()   { throw new UnsupportedOperationException("hashCode is not supported!"); } // EQUIVALENCE_MOD
      public String toString(){ return "Node: " + key + "=" + val; }
      public final V setValue(V value) {
         throw new UnsupportedOperationException();
      }

      public final boolean equals(Object o) {
         throw new UnsupportedOperationException("equals is not supported!");
      }

      /**
       * Virtualized support for map.get(); overridden in subclasses.
       */
      Node<K,V> find(int h, Object k) {
         Node<K,V> e = this;
         if (k != null) {
            do {
               K ek;
               if (e.hash == h &&
                     ((ek = e.key) == k || (ek != null && nodeEq.keyEq.equals(ek, k)))) // EQUIVALENCE_MOD
                  return e;
            } while ((e = e.next) != null);
         }
         return null;
      }

      void lazySetEviction(EvictionEntry<K, V> val) {
         UNSAFE.putOrderedObject(this, evictionOffset, val);
     }

      // Unsafe mechanics

      private static final sun.misc.Unsafe UNSAFE;
      private static final long evictionOffset;

      static {
          try {
              UNSAFE = BoundedEquivalentConcurrentHashMapV8.getUnsafe();
              Class<?> k = Node.class;
              evictionOffset = UNSAFE.objectFieldOffset
                  (k.getDeclaredField("eviction"));
          } catch (Exception e) {
              throw new Error(e);
          }
      }
   }

    /* ---------------- Static utilities -------------- */

   /**
    * Spreads (XORs) higher bits of hash to lower and also forces top
    * bit to 0. Because the table uses power-of-two masking, sets of
    * hashes that vary only in bits above the current mask will
    * always collide. (Among known examples are sets of Float keys
    * holding consecutive whole numbers in small tables.)  So we
    * apply a transform that spreads the impact of higher bits
    * downward. There is a tradeoff between speed, utility, and
    * quality of bit-spreading. Because many common sets of hashes
    * are already reasonably distributed (so don't benefit from
    * spreading), and because we use trees to handle large sets of
    * collisions in bins, we just XOR some shifted bits in the
    * cheapest possible way to reduce systematic lossage, as well as
    * to incorporate impact of the highest bits that would otherwise
    * never be used in index calculations because of table bounds.
    */
   static final int spread(int h) {
      return (h ^ (h >>> 16)) & HASH_BITS;
   }

   /**
    * Returns a power of two table size for the given desired capacity.
    * See Hackers Delight, sec 3.2
    */
   private static final int tableSizeFor(int c) {
      int n = c - 1;
      n |= n >>> 1;
      n |= n >>> 2;
      n |= n >>> 4;
      n |= n >>> 8;
      n |= n >>> 16;
      return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
   }

   /**
    * Returns x's Class if it is of the form "class C implements
    * Comparable<C>", else null.
    */
   static Class<?> comparableClassFor(Object x, Equivalence<?> eq) { // EQUIVALENCE_MOD
      if (eq.isComparable(x)) { // EQUIVALENCE_MOD
         Class<?> c; Type[] ts, as; Type t; ParameterizedType p;
         if ((c = x.getClass()) == String.class) // bypass checks
            return c;
         if ((ts = c.getGenericInterfaces()) != null) {
            for (int i = 0; i < ts.length; ++i) {
               if (((t = ts[i]) instanceof ParameterizedType) &&
                     ((p = (ParameterizedType)t).getRawType() ==
                            Comparable.class) &&
                     (as = p.getActualTypeArguments()) != null &&
                     as.length == 1 && as[0] == c) // type arg is c
                  return c;
            }
         }
      }
      return null;
   }

   /**
    * Returns k.compareTo(x) if x matches kc (k's screened comparable
    * class), else 0.
    */
   static int compareComparables(Class<?> kc, Object k, Object x, Equivalence<Object> eq) { // EQUIVALENCE_MOD
      return (x == null || x.getClass() != kc ? 0 : eq.compare(k, x)); // EQUIVALENCE_MOD
   }

    /* ---------------- Table element access -------------- */

    /*
     * Volatile access methods are used for table elements as well as
     * elements of in-progress next table while resizing.  All uses of
     * the tab arguments must be null checked by callers.  All callers
     * also paranoically precheck that tab's length is not zero (or an
     * equivalent check), thus ensuring that any index argument taking
     * the form of a hash value anded with (length - 1) is a valid
     * index.  Note that, to be correct wrt arbitrary concurrency
     * errors by users, these checks must operate on local variables,
     * which accounts for some odd-looking inline assignments below.
     * Note that calls to setTabAt always occur within locked regions,
     * and so in principle require only release ordering, not
     * full volatile semantics, but are currently coded as volatile
     * writes to be conservative.
     */

   @SuppressWarnings("unchecked")
   static final <K,V> Node<K,V> tabAt(Node<K,V>[] tab, int i) {
      return (Node<K,V>)U.getObjectVolatile(tab, ((long)i << ASHIFT) + ABASE);
   }

   static final <K,V> boolean casTabAt(Node<K,V>[] tab, int i,
         Node<K,V> c, Node<K,V> v) {
      return U.compareAndSwapObject(tab, ((long)i << ASHIFT) + ABASE, c, v);
   }

   static final <K,V> void setTabAt(Node<K,V>[] tab, int i, Node<K,V> v) {
      U.putObjectVolatile(tab, ((long)i << ASHIFT) + ABASE, v);
   }

    /* ---------------- Fields -------------- */

   /**
    * The array of bins. Lazily initialized upon first insertion.
    * Size is always a power of two. Accessed directly by iterators.
    */
   transient volatile Node<K,V>[] table;

   /**
    * The next table to use; non-null only while resizing.
    */
   private transient volatile Node<K,V>[] nextTable;

   /**
    * Base counter value, used mainly when there is no contention,
    * but also as a fallback during table initialization
    * races. Updated via CAS.
    */
   private transient volatile long baseCount;

   /**
    * Table initialization and resizing control.  When negative, the
    * table is being initialized or resized: -1 for initialization,
    * else -(1 + the number of active resizing threads).  Otherwise,
    * when table is null, holds the initial table size to use upon
    * creation, or 0 for default. After initialization, holds the
    * next element count value upon which to resize the table.
    */
   private transient volatile int sizeCtl;

   /**
    * The next table index (plus one) to split while resizing.
    */
   private transient volatile int transferIndex;

   /**
    * Spinlock (locked via CAS) used when resizing and/or creating CounterCells.
    */
   private transient volatile int cellsBusy;

   /**
    * Table of counter cells. When non-null, size is a power of 2.
    */
   private transient volatile CounterCell[] counterCells;

   // views
   private transient KeySetView<K,V> keySet;
   private transient ValuesView<K,V> values;
   private transient EntrySetView<K,V> entrySet;

   Equivalence<? super K> keyEq;
   Equivalence<? super V> valueEq;
   transient NodeEquivalence<K, V> nodeEq;

   final long maxSize;

   final EvictionPolicy<K, V> evictionPolicy;
   final EvictionListener<? super K, ? super V> evictionListener;

    /* ---------------- Public operations -------------- */

   /**
    * Creates a new, empty map with the default initial table size (16).
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      if (maxSize <= 0) {
         throw new IllegalArgumentException();
      }
      this.maxSize = maxSize;
      this.keyEq = keyEquivalence; // EQUIVALENCE_MOD
      this.valueEq = valueEquivalence; // EQUIVALENCE_MOD
      this.nodeEq = new NodeEquivalence<K, V>(this.keyEq, this.valueEq); // EQUIVALENCE_MOD
      this.evictionPolicy = Eviction.LRU.make(this, null, maxSize);
      this.evictionListener = new NullEvictionListener<K, V>();
   }

   /**
    * Creates a new, empty map with the default initial table size (16).
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize,
         Eviction evictionStrategy, EvictionListener<? super K, ? super V> evictionListener,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, evictionStrategy, evictionListener, keyEquivalence, valueEquivalence,
            null);
   }

   /**
    * Creates a new, empty map with the default initial table size (16).
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize,
         Eviction evictionStrategy, EvictionListener<? super K, ? super V> evictionListener,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence,
         EntrySizeCalculator<? super K, ? super V> sizeCalculator) {
      if (maxSize <= 0) {
         throw new IllegalArgumentException();
      }
      if (evictionStrategy == null || evictionListener == null) {
         throw new NullPointerException();
      }
      this.maxSize = maxSize;
      this.keyEq = keyEquivalence; // EQUIVALENCE_MOD
      this.valueEq = valueEquivalence; // EQUIVALENCE_MOD
      this.nodeEq = new NodeEquivalence<K, V>(this.keyEq, this.valueEq); // EQUIVALENCE_MOD
      this.evictionPolicy = evictionStrategy.make(this, sizeCalculator, maxSize);
      this.evictionListener = evictionListener;
   }

   /**
    * Creates a new, empty map with an initial table size
    * accommodating the specified number of elements without the need
    * to dynamically resize.
    *
    * @param initialCapacity The implementation performs internal
    * sizing to accommodate this many elements.
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize, int initialCapacity,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, keyEquivalence, valueEquivalence); // EQUIVALENCE_MOD
      if (initialCapacity < 0)
         throw new IllegalArgumentException();
      if (initialCapacity > maxSize) {
         // Since initialCapacity has to be a power of 2 we shouldn't really set it to
         // maxSize since it can be any arbitrary size
         throw new IllegalArgumentException("initialCapacity cannot be greater than maxSize");
      }
      int cap = ((initialCapacity >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
                       tableSizeFor(initialCapacity + (initialCapacity >>> 1) + 1));
      this.sizeCtl = cap;
   }

   /**
    * Creates a new, empty map with an initial table size
    * accommodating the specified number of elements without the need
    * to dynamically resize.
    *
    * @param initialCapacity The implementation performs internal
    * sizing to accommodate this many elements.
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize, int initialCapacity,
         Eviction evictionStrategy, EvictionListener<? super K, ? super V> evictionListener,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, evictionStrategy, evictionListener, keyEquivalence, valueEquivalence); // EQUIVALENCE_MOD
      
      if (initialCapacity < 0)
         throw new IllegalArgumentException();
      if (initialCapacity > maxSize) {
         // Since initialCapacity has to be a power of 2 we shouldn't really set it to
         // maxSize since it can be any arbitrary size
         throw new IllegalArgumentException("initialCapacity cannot be greater than maxSize");
      }
      int cap = ((initialCapacity >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
                       tableSizeFor(initialCapacity + (initialCapacity >>> 1) + 1));
      this.sizeCtl = cap;
   }

   /**
    * Creates a new map with the same mappings as the given map.
    *
    * @param m the map
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize, Map<? extends K, ? extends V> m,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, keyEquivalence, valueEquivalence); // EQUIVALENCE_MOD
      this.sizeCtl = DEFAULT_CAPACITY;
      putAll(m);
   }

   /**
    * Creates a new, empty map with an initial table size based on
    * the given number of elements ({@code initialCapacity}) and
    * initial table density ({@code loadFactor}).
    *
    * @param initialCapacity the initial capacity. The implementation
    * performs internal sizing to accommodate this many elements,
    * given the specified load factor.
    * @param loadFactor the load factor (table density) for
    * establishing the initial table size
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative or the load factor is nonpositive
    *
    * @since 1.6
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize, int initialCapacity, float loadFactor,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, initialCapacity, loadFactor, 1, Eviction.LRU, getNullEvictionListener(), 
            keyEquivalence, valueEquivalence); // EQUIVALENCE_MOD
   }

   /**
    * Creates a new, empty map with an initial table size based on
    * the given number of elements ({@code initialCapacity}), table
    * density ({@code loadFactor}), and number of concurrently
    * updating threads ({@code concurrencyLevel}).
    *
    * @param initialCapacity the initial capacity. The implementation
    * performs internal sizing to accommodate this many elements,
    * given the specified load factor.
    * @param loadFactor the load factor (table density) for
    * establishing the initial table size
    * @param concurrencyLevel the estimated number of concurrently
    * updating threads. The implementation may use this value as
    * a sizing hint.
    * @throws IllegalArgumentException if the initial capacity is
    * negative or the load factor or concurrencyLevel are
    * nonpositive
    */
   public BoundedEquivalentConcurrentHashMapV8(long maxSize, int initialCapacity,
         float loadFactor, int concurrencyLevel,
         Eviction evictionStrategy, EvictionListener<? super K, ? super V> evictionListener,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      this(maxSize, initialCapacity, evictionStrategy, evictionListener, keyEquivalence, valueEquivalence); // EQUIVALENCE_MOD
      if (!(loadFactor > 0.0f) || initialCapacity < 0 || concurrencyLevel <= 0)
         throw new IllegalArgumentException();
      if (initialCapacity < concurrencyLevel)   // Use at least as many bins
         initialCapacity = concurrencyLevel;   // as estimated threads
      long size = (long)(1.0 + (long)initialCapacity / loadFactor);
      int cap = (size >= (long)MAXIMUM_CAPACITY) ?
            MAXIMUM_CAPACITY : tableSizeFor((int)size);
      this.sizeCtl = cap;
   }

   // Original (since JDK1.2) Map methods

   /**
    * {@inheritDoc}
    */
   public int size() {
      long n = sumCount();
      return ((n < 0L) ? 0 :
                    (n > (long)Integer.MAX_VALUE) ? Integer.MAX_VALUE :
                          (int)n);
   }

   /**
    * {@inheritDoc}
    */
   public boolean isEmpty() {
      return sumCount() <= 0L; // ignore transient negative values
   }

   /**
    * Returns the value to which the specified key is mapped,
    * or {@code null} if this map contains no mapping for the key.
    *
    * <p>More formally, if this map contains a mapping from a key
    * {@code k} to a value {@code v} such that {@code key.equals(k)},
    * then this method returns {@code v}; otherwise it returns
    * {@code null}.  (There can be at most one such mapping.)
    *
    * @throws NullPointerException if the specified key is null
    */
   public V get(Object key) {
      Node<K,V>[] tab; Node<K,V> e, p; int n, eh; K ek;
      int h = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      if ((tab = table) != null && (n = tab.length) > 0 &&
            (e = tabAt(tab, (n - 1) & h)) != null) {
         if ((eh = e.hash) == h) {
            if ((ek = e.key) == key || (ek != null && keyEq.equals(ek, key))) {// EQUIVALENCE_MOD
               V val = e.val;
               if (val != NULL_VALUE) {
                  evictionPolicy.onEntryHitRead(e, val);
                  notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
                  return val;
               }
               return null;
            }
         }
         else if (eh < 0) {
            V val = (p = e.find(h, key)) != null ? p.val : null;
            if (val != null && val != NULL_VALUE) {
               evictionPolicy.onEntryHitRead(p, val);
               notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
               return val;
            }
            return null;
         }
         while ((e = e.next) != null) {
            if (e.hash == h &&
                  ((ek = e.key) == key || (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
               V val = e.val;
               if (val != NULL_VALUE) {
                  evictionPolicy.onEntryHitRead(e, val);
                  notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
                  return val;
               }
               return null;
            }
         }
      }
      return null;
   }

   @Override
   public V peek(Object key) {
      V val = innerPeek(key);
      return val == NULL_VALUE ? null : val;
   }

   V innerPeek(Object key) {
      Node<K,V>[] tab; Node<K,V> e, p; int n, eh; K ek;
      int h = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      if ((tab = table) != null && (n = tab.length) > 0 &&
            (e = tabAt(tab, (n - 1) & h)) != null) {
         if ((eh = e.hash) == h) {
            if ((ek = e.key) == key || (ek != null && keyEq.equals(ek, key))) {// EQUIVALENCE_MOD
               return e.val;
            }
         }
         else if (eh < 0) {
            V val = (p = e.find(h, key)) != null ? p.val : null;
            return val;
         }
         while ((e = e.next) != null) {
            if (e.hash == h &&
                  ((ek = e.key) == key || (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
               return e.val;
            }
         }
      }
      return null;
   }

   /**
    * Tests if the specified object is a key in this table.
    *
    * @param  key possible key
    * @return {@code true} if and only if the specified object
    *         is a key in this table, as determined by the
    *         {@code equals} method; {@code false} otherwise
    * @throws NullPointerException if the specified key is null
    */
   public boolean containsKey(Object key) {
      return get(key) != null;
   }

   /**
    * Returns {@code true} if this map maps one or more keys to the
    * specified value. Note: This method may require a full traversal
    * of the map, and is much slower than method {@code containsKey}.
    *
    * @param value value whose presence in this map is to be tested
    * @return {@code true} if this map maps one or more keys to the
    *         specified value
    * @throws NullPointerException if the specified value is null
    */
   public boolean containsValue(Object value) {
      if (value == null)
         throw new NullPointerException();
      Node<K,V>[] t;
      if ((t = table) != null) {
         Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V v;
            if ((v = p.val) == value || (v != null && v != NULL_VALUE && valueEq.equals(v, value))) // EQUIVALENCE_MOD
               return true;
         }
      }
      return false;
   }
   /**
    * Maps the specified key to the specified value in this table.
    * Neither the key nor the value can be null.
    *
    * <p>The value can be retrieved by calling the {@code get} method
    * with a key that is equal to the original key.
    *
    * @param key key with which the specified value is to be associated
    * @param value value to be associated with the specified key
    * @return the previous value associated with {@code key}, or
    *         {@code null} if there was no mapping for {@code key}
    * @throws NullPointerException if the specified key or value is null
    */
   public V put(K key, V value) {
      return putVal(key, value, false);
   }

   /** Implementation for put and putIfAbsent */
   final V putVal(K key, V value, boolean onlyIfAbsent) {
      if (key == null || value == null) throw new NullPointerException();
      int hash = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      int binCount = 0;
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0)
            tab = initTable();
         else if ((f = tabAt(tab, i = (n - 1) & hash)) == null) {
            Node<K, V> newNode = evictionPolicy.createNewEntry(key, hash, null, value, null);
            synchronized (newNode) {
               if (casTabAt(tab, i, null, newNode)) {// EQUIVALENCE_MOD
                  evictionPolicy.onEntryMiss(newNode, value);
                  evictionListener.onEntryActivated(key);
                  break;
               }
            }
         }
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            V oldVal = null;
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     binCount = 1;
                     for (Node<K,V> e = f;; ++binCount) {
                        K ek;
                        if (e.hash == hash &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           oldVal = e.val;
                           if (!onlyIfAbsent || oldVal == NULL_VALUE) {
                              e.val = value;
                              // We support null evicted values
                              if (oldVal == NULL_VALUE) {
                                 evictionPolicy.onEntryMiss(e, value);
                              } else {
                                 evictionPolicy.onEntryHitWrite(e, value);
                              }
                           }
                           break;
                        }
                        Node<K,V> pred = e;
                        if ((e = e.next) == null) {
                           pred.next = evictionPolicy.createNewEntry(key,  hash,  null, 
                                 value, null);
                           evictionPolicy.onEntryMiss(pred.next, value);
                           // When entry not present, attempt to activate if necessary
                           evictionListener.onEntryActivated(key);
                           break;
                        }
                     }
                  }
                  else if (f instanceof TreeBin) {
                     Node<K,V> p;
                     binCount = 2;
                     if ((p = ((TreeBin<K,V>)f).putTreeVal(hash, key,
                           value)) != null) {
                        oldVal = p.val;
                        if (!onlyIfAbsent || oldVal == NULL_VALUE) {
                           p.val = value;
                           // We support null evicted values
                           if (oldVal == NULL_VALUE) {
                              evictionPolicy.onEntryMiss(p, value);
                           } else {
                              evictionPolicy.onEntryHitWrite(p, value);
                           }
                        }
                     }
                  }
               }
            }
            if (binCount != 0) {
               if (binCount >= TREEIFY_THRESHOLD)
                  treeifyBin(tab, i);
               if (oldVal == null || oldVal == NULL_VALUE) {
                  break;
               }
               notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
               return oldVal;
            }
         }
      }
      addCount(1L, binCount);
      notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
      return null;
   }

   /**
    * Copies all of the mappings from the specified map to this one.
    * These mappings replace any mappings that this map had for any of the
    * keys currently in the specified map.
    *
    * @param m mappings to be stored in this map
    */
   public void putAll(Map<? extends K, ? extends V> m) {
      tryPresize(m.size());
      for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
         putVal(e.getKey(), e.getValue(), false);
   }

   /**
    * Removes the key (and its corresponding value) from this map.
    * This method does nothing if the key is not in the map.
    *
    * @param  key the key that needs to be removed
    * @return the previous value associated with {@code key}, or
    *         {@code null} if there was no mapping for {@code key}
    * @throws NullPointerException if the specified key is null
    */
   public V remove(Object key) {
      return replaceNode(key, null, null);
   }

   final V replaceNode(Object key, V value, Object cv) {
      return replaceNode(key, value, cv, false);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   private void notifyListenerOfRemoval(Node removedNode, boolean isEvict) {
      if (isEvict) {
         evictionListener.onEntryChosenForEviction(removedNode);
      } else {
         evictionListener.onEntryRemoved(removedNode);
      }
   }

   /**
    * Implementation for the four public remove/replace methods:
    * Replaces node value with v, conditional upon match of cv if
    * non-null.  If resulting value is null, delete.
    */
   final V replaceNode(Object key, V value, Object cv, boolean isEvict) {
      int hash = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0 ||
               (f = tabAt(tab, i = (n - 1) & hash)) == null)
            break;
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            V oldVal = null;
            boolean validated = false;
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     validated = true;
                     for (Node<K,V> e = f, pred = null;;) {
                        K ek;
                        if (e.hash == hash &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           V ev = e.val == NULL_VALUE ? null : e.val;
                           if (cv == null || cv == ev ||
                                 (ev != null && valueEq.equals(ev, cv))) { // EQUIVALENCE_MOD
                              oldVal = ev;
                              if (value != null) {
                                 e.val = value;
                                 if (oldVal == null) {
                                    evictionPolicy.onEntryMiss(e, value);
                                 } else {
                                    evictionPolicy.onEntryHitWrite(e, value);
                                 }
                              }
                              else if (pred != null) {
                                 if (!isEvict) {
                                    evictionPolicy.onEntryRemove(e);
                                 }
                                 if (oldVal != null) {
                                    notifyListenerOfRemoval(e, isEvict);
                                 }
                                 pred.next = e.next;
                              }
                              else {
                                 if (!isEvict) {
                                    evictionPolicy.onEntryRemove(e);
                                 }
                                 if (oldVal != null) {
                                    notifyListenerOfRemoval(e, isEvict);
                                 }
                                 setTabAt(tab, i, e.next);
                              }
                           }
                           break;
                        }
                        pred = e;
                        if ((e = e.next) == null)
                           break;
                     }
                  }
                  else if (f instanceof TreeBin) {
                     validated = true;
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> r, p;
                     if ((r = t.root) != null &&
                           (p = r.findTreeNode(hash, key, null)) != null) {
                        V pv = p.val == NULL_VALUE ? null : p.val;
                        if (cv == null || cv == pv ||
                              (pv != null && valueEq.equals(pv, cv))) { // EQUIVALENCE_MOD
                           oldVal = pv;
                           if (value != null) {
                              p.val = value;
                              if (oldVal == null) {
                                 evictionPolicy.onEntryMiss(p, value);
                              } else {
                                 evictionPolicy.onEntryHitWrite(p, value);
                              }
                           }
                           else {
                              if (t.removeTreeNode(p)) {
                                 setTabAt(tab, i, untreeify(t.first)); // EQUIVALENCE_MOD
                              }
                              if (!isEvict) {
                                 evictionPolicy.onEntryRemove(p);
                              }
                              if (pv != null) {
                                notifyListenerOfRemoval(p, isEvict);
                              }
                           }
                        }
                     }
                  }
               }
            }
            if (validated) {
               if (oldVal != null) {
                  if (value == null)
                     addCount(-1L, -1);
                  return oldVal;
               }
               break;
            }
         }
      }
      if (!isEvict) {
         notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
      }
      return null;
   }

   /**
    * Removes all of the mappingsonEntryHit(e) from this map.
    */
   public void clear() {
      long delta = 0L; // negative number of deletions
      int i = 0;
      Node<K,V>[] tab = table;
      while (tab != null && i < tab.length) {
         int fh;
         Node<K,V> f = tabAt(tab, i);
         if (f == null)
            ++i;
         else if ((fh = f.hash) == MOVED) {
            tab = helpTransfer(tab, f);
            i = 0; // restart
         }
         else {
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  Node<K,V> p = (fh >= 0 ? f :
                                       (f instanceof TreeBin) ?
                                             ((TreeBin<K,V>)f).first : null);
                  while (p != null) {
                     if (p.val != NULL_VALUE) {
                        --delta;
                     }
                     evictionPolicy.onEntryRemove(p);
                     p = p.next;
                  }
                  setTabAt(tab, i++, null);
               }
            }
         }
      }
      if (delta != 0L)
         addCount(delta, -1);
   }

   /**
    * Returns a {@link Set} view of the keys contained in this map.
    * The set is backed by the map, so changes to the map are
    * reflected in the set, and vice-versa. The set supports element
    * removal, which removes the corresponding mapping from this map,
    * via the {@code Iterator.remove}, {@code Set.remove},
    * {@code removeAll}, {@code retainAll}, and {@code clear}
    * operations.  It does not support the {@code add} or
    * {@code addAll} operations.
    *
    * <p>The view's {@code iterator} is a "weakly consistent" iterator
    * that will never throw {@link ConcurrentModificationException},
    * and guarantees to traverse elements as they existed upon
    * construction of the iterator, and may (but is not guaranteed to)
    * reflect any modifications subsequent to construction.
    *
    * @return the set view
    */
   public KeySetView<K,V> keySet() {
      KeySetView<K,V> ks;
      return (ks = keySet) != null ? ks : (keySet = new KeySetView<K,V>(this, null));
   }

   /**
    * Returns a {@link Collection} view of the values contained in this map.
    * The collection is backed by the map, so changes to the map are
    * reflected in the collection, and vice-versa.  The collection
    * supports element removal, which removes the corresponding
    * mapping from this map, via the {@code Iterator.remove},
    * {@code Collection.remove}, {@code removeAll},
    * {@code retainAll}, and {@code clear} operations.  It does not
    * support the {@code add} or {@code addAll} operations.
    *
    * <p>The view's {@code iterator} is a "weakly consistent" iterator
    * that will never throw {@link ConcurrentModificationException},
    * and guarantees to traverse elements as they existed upon
    * construction of the iterator, and may (but is not guaranteed to)
    * reflect any modifications subsequent to construction.
    *
    * @return the collection view
    */
   public Collection<V> values() {
      ValuesView<K,V> vs;
      return (vs = values) != null ? vs : (values = new ValuesView<K,V>(this));
   }

   /**
    * Returns a {@link Set} view of the mappings contained in this map.
    * The set is backed by the map, so changes to the map are
    * reflected in the set, and vice-versa.  The set supports element
    * removal, which removes the corresponding mapping from the map,
    * via the {@code Iterator.remove}, {@code Set.remove},
    * {@code removeAll}, {@code retainAll}, and {@code clear}
    * operations.
    *
    * <p>The view's {@code iterator} is a "weakly consistent" iterator
    * that will never throw {@link ConcurrentModificationException},
    * and guarantees to traverse elements as they existed upon
    * construction of the iterator, and may (but is not guaranteed to)
    * reflect any modifications subsequent to construction.
    *
    * @return the set view
    */
   public Set<Map.Entry<K,V>> entrySet() {
      EntrySetView<K,V> es;
      return (es = entrySet) != null ? es : (entrySet = new EntrySetView<K,V>(this));
   }

   /**
    * Returns the hash code value for this {@link Map}, i.e.,
    * the sum of, for each key-value pair in the map,
    * {@code key.hashCode() ^ value.hashCode()}.
    *
    * @return the hash code value for this map
    */
   public int hashCode() {
      int h = 0;
      Node<K,V>[] t;
      if ((t = table) != null) {
         Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V val = p.val;
            if (val != NULL_VALUE) {
               h += p.hashCode(p.key, val); // EQUIVALENCE_MOD
            }
         }
      }
      return h;
   }

   /**
    * Returns a string representation of this map.  The string
    * representation consists of a list of key-value mappings (in no
    * particular order) enclosed in braces ("{@code {}}").  Adjacent
    * mappings are separated by the characters {@code ", "} (comma
    * and space).  Each key-value mapping is rendered as the key
    * followed by an equals sign ("{@code =}") followed by the
    * associated value.
    *
    * @return a string representation of this map
    */
   public String toString() {
      Node<K,V>[] t;
      int f = (t = table) == null ? 0 : t.length;
      Traverser<K,V> it = new Traverser<K,V>(t, f, 0, f);
      StringBuilder sb = new StringBuilder();
      sb.append('{');
      Node<K,V> p;
      if ((p = it.advance()) != null) {
         for (;;) {
            K k = p.key;
            V v = p.val;
            if (v != NULL_VALUE) {
               sb.append(k == this ? "(this Map)" : keyEq.toString(k)); // EQUIVALENCE_MOD
               sb.append('=');
               sb.append(v == this ? "(this Map)" : valueEq.toString(v)); // EQUIVALENCE_MOD
            }
            if ((p = it.advance()) == null)
               break;
            if (v != NULL_VALUE) {
               sb.append(',').append(' ');
            }
         }
      }
      return sb.append('}').toString();
   }

   /**
    * Compares the specified object with this map for equality.
    * Returns {@code true} if the given object is a map with the same
    * mappings as this map.  This operation may return misleading
    * results if either map is concurrently modified during execution
    * of this method.
    *
    * @param o object to be compared for equality with this map
    * @return {@code true} if the specified object is equal to this map
    */
   @SuppressWarnings("unchecked")
   public boolean equals(Object o) {
      if (o != this) {
         if (!(o instanceof Map))
            return false;
         Map<?,?> m = (Map<?,?>) o;
         Node<K,V>[] t;
         int f = (t = table) == null ? 0 : t.length;
         Traverser<K,V> it = new Traverser<K,V>(t, f, 0, f);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V val = p.val;
            if (val != NULL_VALUE) {
               Object v = m.get(p.key);
               if (v == null || (v != val && !valueEq.equals(val, v))) // EQUIVALENCE_MOD
                 return false;
            }
         }
         for (Map.Entry<?,?> e : m.entrySet()) {
            Object mk, mv, v;
            if ((mk = e.getKey()) == null ||
                  (mv = e.getValue()) == null ||
                  (v = get(mk)) == null ||
                  (mv != v && !valueEq.equals((V) v, mv))) // EQUIVALENCE_MOD
               return false;
         }
      }
      return true;
   }

   /**
    * Stripped-down version of helper class used in previous version,
    * declared for the sake of serialization compatibility
    */
   static class Segment<K,V> extends ReentrantLock implements Serializable {
      private static final long serialVersionUID = 2249069246763182397L;
      final float loadFactor;
      Segment(float lf) { this.loadFactor = lf; }
   }

   /**
    * Saves the state of the {@code EquivalentConcurrentHashMapV8} instance to a
    * stream (i.e., serializes it).
    * @param s the stream
    * @throws java.io.IOException if an I/O error occurs
    * @serialData
    * the key (Object) and value (Object)
    * for each key-value mapping, followed by a null pair.
    * The key-value mappings are emitted in no particular order.
    */
   @SuppressWarnings("unchecked")
   private void writeObject(java.io.ObjectOutputStream s)
         throws java.io.IOException {
      // For serialization compatibility
      // Emulate segment calculation from previous version of this class
      int sshift = 0;
      int ssize = 1;
      while (ssize < DEFAULT_CONCURRENCY_LEVEL) {
         ++sshift;
         ssize <<= 1;
      }
      int segmentShift = 32 - sshift;
      int segmentMask = ssize - 1;
      Segment<K,V>[] segments = (Segment<K,V>[])
            new Segment<?,?>[DEFAULT_CONCURRENCY_LEVEL];
      for (int i = 0; i < segments.length; ++i)
         segments[i] = new Segment<K,V>(LOAD_FACTOR);
      s.putFields().put("segments", segments);
      s.putFields().put("segmentShift", segmentShift);
      s.putFields().put("segmentMask", segmentMask);
      s.writeFields();

      s.writeObject(keyEq); // EQUIVALENCE_MOD
      s.writeObject(valueEq); // EQUIVALENCE_MOD

      Node<K,V>[] t;
      if ((t = table) != null) {
         Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V val = p.val;
            if (val != null) {
               s.writeObject(p.key);
               s.writeObject(val);
            }
         }
      }
      s.writeObject(null);
      s.writeObject(null);
      segments = null; // throw away
   }

   /**
    * Reconstitutes the instance from a stream (that is, deserializes it).
    * @param s the stream
    * @throws ClassNotFoundException if the class of a serialized object
    *         could not be found
    * @throws java.io.IOException if an I/O error occurs
    */
   @SuppressWarnings("unchecked")
   private void readObject(java.io.ObjectInputStream s)
         throws java.io.IOException, ClassNotFoundException {
      /*
       * To improve performance in typical cases, we create nodes
       * while reading, then place in table once size is known.
       * However, we must also validate uniqueness and deal with
       * overpopulated bins while doing so, which requires
       * specialized versions of putVal mechanics.
       */
      sizeCtl = -1; // force exclusion for table construction
      s.defaultReadObject();

      keyEq = (Equivalence<K>) s.readObject(); // EQUIVALENCE MOD
      valueEq = (Equivalence<V>) s.readObject(); // EQUIVALENCE MOD
      nodeEq = new NodeEquivalence<K, V>(keyEq, valueEq);

      long size = 0L;
      Node<K,V> p = null;
      for (;;) {
         K k = (K) s.readObject();
         V v = (V) s.readObject();
         if (k != null && v != null) {
            p = evictionPolicy.createNewEntry(k, spread(keyEq.hashCode(k)), p, v, null); // EQUIVALENCE_MOD
            evictionPolicy.onEntryMiss(p, v);
            size -= evictionPolicy.findIfEntriesNeedEvicting().size();
            ++size;
         }
         else
            break;
      }
      if (size == 0L)
         sizeCtl = 0;
      else {
         int n;
         if (size >= (long)(MAXIMUM_CAPACITY >>> 1))
            n = MAXIMUM_CAPACITY;
         else {
            int sz = (int)size;
            n = tableSizeFor(sz + (sz >>> 1) + 1);
         }
         Node<K,V>[] tab = (Node<K,V>[])new Node<?,?>[n];
         int mask = n - 1;
         long added = 0L;
         while (p != null) {
            boolean insertAtFront;
            Node<K,V> next = p.next, first;
            int h = p.hash, j = h & mask;
            if ((first = tabAt(tab, j)) == null)
               insertAtFront = true;
            else {
               K k = p.key;
               if (first.hash < 0) {
                  TreeBin<K,V> t = (TreeBin<K,V>)first;
                  if (t.putTreeVal(h, k, p.val) == null)
                     ++added;
                  insertAtFront = false;
               }
               else {
                  int binCount = 0;
                  insertAtFront = true;
                  Node<K,V> q; K qk;
                  for (q = first; q != null; q = q.next) {
                     if (q.hash == h &&
                           ((qk = q.key) == k ||
                                  (qk != null && keyEq.equals(qk, k)))) { // EQUIVALENCE_MOD
                        insertAtFront = false;
                        break;
                     }
                     ++binCount;
                  }
                  if (insertAtFront && binCount >= TREEIFY_THRESHOLD) {
                     insertAtFront = false;
                     ++added;
                     p.next = first;
                     TreeNode<K,V> hd = null, tl = null;
                     for (q = p; q != null; q = q.next) {
                        TreeNode<K,V> t = evictionPolicy.createNewEntry(q.key, q.hash, 
                              null, null, q.val, q.eviction);
                        if ((t.prev = tl) == null)
                           hd = t;
                        else
                           tl.next = t;
                        tl = t;
                     }
                     setTabAt(tab, j, new TreeBin<K,V>(hd, this)); // EQUIVALENCE_MOD
                  }
               }
            }
            if (insertAtFront) {
               ++added;
               p.next = first;
               setTabAt(tab, j, p);
            }
            p = next;
         }
         table = tab;
         sizeCtl = n - (n >>> 2);
         baseCount = added;
      }
   }

   // ConcurrentMap methods

   /**
    * {@inheritDoc}
    *
    * @return the previous value associated with the specified key,
    *         or {@code null} if there was no mapping for the key
    * @throws NullPointerException if the specified key or value is null
    */
   public V putIfAbsent(K key, V value) {
      return putVal(key, value, true);
   }

   /**
    * {@inheritDoc}
    *
    * @throws NullPointerException if the specified key is null
    */
   public boolean remove(Object key, Object value) {
      if (key == null)
         throw new NullPointerException();
      return value != null && replaceNode(key, null, value) != null;
   }

   /**
    * {@inheritDoc}
    *
    * @throws NullPointerException if any of the arguments are null
    */
   public boolean replace(K key, V oldValue, V newValue) {
      if (key == null || oldValue == null || newValue == null)
         throw new NullPointerException();
      return replaceNode(key, newValue, oldValue) != null;
   }

   /**
    * {@inheritDoc}
    *
    * @return the previous value associated with the specified key,
    *         or {@code null} if there was no mapping for the key
    * @throws NullPointerException if the specified key or value is null
    */
   public V replace(K key, V value) {
      if (key == null || value == null)
         throw new NullPointerException();
      return replaceNode(key, value, null);
   }

   // Overrides of JDK8+ Map extension method defaults

   /**
    * Returns the value to which the specified key is mapped, or the
    * given default value if this map contains no mapping for the
    * key.
    *
    * @param key the key whose associated value is to be returned
    * @param defaultValue the value to return if this map contains
    * no mapping for the given key
    * @return the mapping for the key, if present; else the default value
    * @throws NullPointerException if the specified key is null
    */
   public V getOrDefault(Object key, V defaultValue) {
      V v;
      return (v = get(key)) == null ? defaultValue : v;
   }

   public void forEach(BiConsumer<? super K, ? super V> action) {
      if (action == null) throw new NullPointerException();
      Node<K,V>[] t;
      if ((t = table) != null) {
         Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V val = p.val;
            if (val != NULL_VALUE)
               action.accept(p.key, val);
         }
      }
   }

   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      if (function == null) throw new NullPointerException();
      Node<K,V>[] t;
      if ((t = table) != null) {
         Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
         for (Node<K,V> p; (p = it.advance()) != null; ) {
            V oldValue = p.val;
            if (oldValue != NULL_VALUE) {
               for (K key = p.key;;) {
                  V newValue = function.apply(key, oldValue);
                  if (newValue == null)
                     throw new NullPointerException();
                  if (replaceNode(key, newValue, oldValue) != null ||
                        (oldValue = get(key)) == null)
                     break;
               }
            }
         }
      }
   }

   /**
    * If the specified key is not already associated with a value,
    * attempts to compute its value using the given mapping function
    * and enters it into this map unless {@code null}.  The entire
    * method invocation is performed atomically, so the function is
    * applied at most once per key.  Some attempted update operations
    * on this map by other threads may be blocked while computation
    * is in progress, so the computation should be short and simple,
    * and must not attempt to update any other mappings of this map.
    *
    * @param key key with which the specified value is to be associated
    * @param mappingFunction the function to compute a value
    * @return the current (existing or computed) value associated with
    *         the specified key, or null if the computed value is null
    * @throws NullPointerException if the specified key or mappingFunction
    *         is null
    * @throws IllegalStateException if the computation detectably
    *         attempts a recursive update to this map that would
    *         otherwise never complete
    * @throws RuntimeException or Error if the mappingFunction does so,
    *         in which case the mapping is left unestablished
    */
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      if (key == null || mappingFunction == null)
         throw new NullPointerException();
      int h = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      V val = null;
      int binCount = 0;
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0)
            tab = initTable();
         else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
            Node<K,V> r = new ReservationNode<K,V>(nodeEq); // EQUIVALENCE_MOD
            synchronized (r) {
               if (casTabAt(tab, i, null, r)) {
                  binCount = 1;
                  Node<K,V> node = null;
                  try {
                     if ((val = mappingFunction.apply(key)) != null) {
                        node = evictionPolicy.createNewEntry(key, h, null, val, null);
                        evictionPolicy.onEntryMiss(node, val);
                        // When entry not present, attempt to activate if necessary
                        evictionListener.onEntryActivated(key);
                     }
                  } finally {
                     setTabAt(tab, i, node);
                  }
               }
            }
            if (binCount != 0)
               break;
         }
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            boolean added = false;
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     binCount = 1;
                     for (Node<K,V> e = f;; ++binCount) {
                        K ek;
                        if (e.hash == h &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           val = e.val;
                           if (val == NULL_VALUE) {
                              if ((val = mappingFunction.apply(key)) != null) {
                                 added = true;
                                 e.val = val;
                                 evictionPolicy.onEntryMiss(e, val);
                                 evictionListener.onEntryActivated(key);
                              }
                           }
                           break;
                        }
                        Node<K,V> pred = e;
                        if ((e = e.next) == null) {
                           if ((val = mappingFunction.apply(key)) != null) {
                              added = true;
                              pred.next = evictionPolicy.createNewEntry(key, h, null, val,
                                    null);
                              evictionPolicy.onEntryMiss(pred.next, val);
                              evictionListener.onEntryActivated(key);
                           }
                           break;
                        }
                     }
                  }
                  else if (f instanceof TreeBin) {
                     binCount = 2;
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> r, p;
                     if ((r = t.root) != null &&
                           (p = r.findTreeNode(h, key, null)) != null) {
                        val = p.val;
                     }
                     else if ((val = mappingFunction.apply(key)) != null) {
                        added = true;
                        t.putTreeVal(h, key, val);
                     }
                  }
               }
            }
            if (binCount != 0) {
               if (binCount >= TREEIFY_THRESHOLD)
                  treeifyBin(tab, i);
               if (!added)
                  return val;
               break;
            }
         }
      }
      if (val != null) {
         addCount(1L, binCount);
         notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
      }
      return val;
   }

   /**
    * If the value for the specified key is present, attempts to
    * compute a new mapping given the key and its current mapped
    * value.  The entire method invocation is performed atomically.
    * Some attempted update operations on this map by other threads
    * may be blocked while computation is in progress, so the
    * computation should be short and simple, and must not attempt to
    * update any other mappings of this map.
    *
    * @param key key with which a value may be associated
    * @param remappingFunction the function to compute a value
    * @return the new value associated with the specified key, or null if none
    * @throws NullPointerException if the specified key or remappingFunction
    *         is null
    * @throws IllegalStateException if the computation detectably
    *         attempts a recursive update to this map that would
    *         otherwise never complete
    * @throws RuntimeException or Error if the remappingFunction does so,
    *         in which case the mapping is unchanged
    */
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      if (key == null || remappingFunction == null)
         throw new NullPointerException();
      int h = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      V val = null;
      int delta = 0;
      int binCount = 0;
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0)
            tab = initTable();
         else if ((f = tabAt(tab, i = (n - 1) & h)) == null)
            break;
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     binCount = 1;
                     for (Node<K,V> e = f, pred = null;; ++binCount) {
                        K ek;
                        if (e.hash == h &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           V prevVal = e.val;
                           if (prevVal != NULL_VALUE) {
                              val = remappingFunction.apply(key, prevVal);
                              if (val != null) {
                                 e.val = val;
                                 evictionPolicy.onEntryHitWrite(e, val);
                              }
                              else {
                                 delta = -1;
                                 Node<K,V> en = e.next;
                                 if (pred != null)
                                    pred.next = en;
                                 else
                                    setTabAt(tab, i, en);
                                 evictionPolicy.onEntryRemove(e);
                              }
                           }
                           break;
                        }
                        pred = e;
                        if ((e = e.next) == null)
                           break;
                     }
                  }
                  else if (f instanceof TreeBin) {
                     binCount = 2;
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> r, p;
                     if ((r = t.root) != null &&
                           (p = r.findTreeNode(h, key, null)) != null) {
                        val = remappingFunction.apply(key, p.val);
                        if (val != null) {
                           p.val = val;
                           evictionPolicy.onEntryHitWrite(p, val);
                        }
                        else {
                           delta = -1;
                           if (t.removeTreeNode(p))
                              setTabAt(tab, i, untreeify(t.first)); // EQUIVALENCE_MOD
                           evictionPolicy.onEntryRemove(p);
                        }
                     }
                  }
               }
            }
            if (binCount != 0)
               break;
         }
      }
      if (delta != 0) {
         addCount((long)delta, binCount);
      }
      return val;
   }

   /**
    * Attempts to compute a mapping for the specified key and its
    * current mapped value (or {@code null} if there is no current
    * mapping). The entire method invocation is performed atomically.
    * Some attempted update operations on this map by other threads
    * may be blocked while computation is in progress, so the
    * computation should be short and simple, and must not attempt to
    * update any other mappings of this Map.
    *
    * @param key key with which the specified value is to be associated
    * @param remappingFunction the function to compute a value
    * @return the new value associated with the specified key, or null if none
    * @throws NullPointerException if the specified key or remappingFunction
    *         is null
    * @throws IllegalStateException if the computation detectably
    *         attempts a recursive update to this map that would
    *         otherwise never complete
    * @throws RuntimeException or Error if the remappingFunction does so,
    *         in which case the mapping is unchanged
    */
   public V compute(K key,
         BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      if (key == null || remappingFunction == null)
         throw new NullPointerException();
      int h = spread(keyEq.hashCode(key)); // EQUIVALENCE_MOD
      V val = null;
      int delta = 0;
      int binCount = 0;
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0)
            tab = initTable();
         else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
            Node<K,V> r = new ReservationNode<K,V>(nodeEq); // EQUIVALENCE_MOD
            synchronized (r) {
               if (casTabAt(tab, i, null, r)) {
                  binCount = 1;
                  Node<K,V> node = null;
                  try {
                     if ((val = remappingFunction.apply(key, null)) != null) {
                        delta = 1;
                        node = evictionPolicy.createNewEntry(key, h, null, val, null);
                        evictionPolicy.onEntryMiss(node, val);
                        // When entry not present, attempt to activate if necessary
                        evictionListener.onEntryActivated(key);
                     }
                  } finally {
                     setTabAt(tab, i, node);
                  }
               }
            }
            if (binCount != 0)
               break;
         }
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     binCount = 1;
                     for (Node<K,V> e = f, pred = null;; ++binCount) {
                        K ek;
                        if (e.hash == h &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           V oldVal = e.val == NULL_VALUE ? null : e.val;
                           val = remappingFunction.apply(key, oldVal);
                           if (val != null) {
                              e.val = val;
                              if (oldVal == null) {
                                 evictionPolicy.onEntryMiss(e, val);
                              } else {
                                 evictionPolicy.onEntryHitWrite(e, val);
                              }
                           }
                           else {
                              delta = oldVal == null ? 0 : -1;
                              Node<K,V> en = e.next;
                              if (pred != null)
                                 pred.next = en;
                              else
                                 setTabAt(tab, i, en);
                              if (oldVal != null) {
                                 evictionPolicy.onEntryRemove(e);
                              }
                           }
                           break;
                        }
                        pred = e;
                        if ((e = e.next) == null) {
                           val = remappingFunction.apply(key, null);
                           if (val != null) {
                              delta = 1;
                              pred.next = evictionPolicy.createNewEntry(key, h, null,
                                    val, null);
                              evictionPolicy.onEntryMiss(pred.next, val);
                              // When entry not present, attempt to activate if necessary
                              evictionListener.onEntryActivated(key);
                           }
                           break;
                        }
                     }
                  }
                  else if (f instanceof TreeBin) {
                     binCount = 1;
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> r, p;
                     if ((r = t.root) != null)
                        p = r.findTreeNode(h, key, null);
                     else
                        p = null;
                     V pv = (p == null) ? null : p.val == NULL_VALUE ? null : p.val;
                     val = remappingFunction.apply(key, pv);
                     if (val != null) {
                        if (p != null) {
                           delta = p.val == null ? 1 : 0;
                           p.val = val;
                           if (pv == null) {
                              evictionPolicy.onEntryMiss(p, val);
                           } else {
                              evictionPolicy.onEntryHitWrite(p, val);
                           }
                        }
                        else {
                           delta = 1;
                           t.putTreeVal(h, key, val);
                        }
                     }
                     else if (p != null) {
                        delta = p.val == null || p.val == NULL_VALUE ? 0 : -1;
                        if (t.removeTreeNode(p))
                           setTabAt(tab, i, untreeify(t.first)); // EQUIVALENCE_MOD
                        evictionPolicy.onEntryRemove(p);
                     }
                  }
               }
            }
            if (binCount != 0) {
               if (binCount >= TREEIFY_THRESHOLD)
                  treeifyBin(tab, i);
               break;
            }
         }
      }
      if (delta != 0) {
         addCount((long)delta, binCount);
      }
      notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
      return val;
   }

   /**
    * If the specified key is not already associated with a
    * (non-null) value, associates it with the given value.
    * Otherwise, replaces the value with the results of the given
    * remapping function, or removes if {@code null}. The entire
    * method invocation is performed atomically.  Some attempted
    * update operations on this map by other threads may be blocked
    * while computation is in progress, so the computation should be
    * short and simple, and must not attempt to update any other
    * mappings of this Map.
    *
    * @param key key with which the specified value is to be associated
    * @param value the value to use if absent
    * @param remappingFunction the function to recompute a value if present
    * @return the new value associated with the specified key, or null if none
    * @throws NullPointerException if the specified key or the
    *         remappingFunction is null
    * @throws RuntimeException or Error if the remappingFunction does so,
    *         in which case the mapping is unchanged
    */
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      if (key == null || value == null || remappingFunction == null)
         throw new NullPointerException();
      int h = spread(keyEq.hashCode(key));
      V val = null;
      int delta = 0;
      int binCount = 0;
      for (Node<K,V>[] tab = table;;) {
         Node<K,V> f; int n, i, fh;
         if (tab == null || (n = tab.length) == 0)
            tab = initTable();
         else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
            Node<K, V> newNode = evictionPolicy.createNewEntry(key, h, null, val, null);
            synchronized (newNode) {
               if (casTabAt(tab, i, null, newNode)) { // EQUIVALENCE_MOD
                  delta = 1;
                  val = value;
                  evictionPolicy.onEntryMiss(newNode, val);
                  evictionListener.onEntryActivated(key);
                  break;
               }
            }
         }
         else if ((fh = f.hash) == MOVED)
            tab = helpTransfer(tab, f);
         else {
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  if (fh >= 0) {
                     binCount = 1;
                     for (Node<K,V> e = f, pred = null;; ++binCount) {
                        K ek;
                        if (e.hash == h &&
                              ((ek = e.key) == key ||
                                     (ek != null && keyEq.equals(ek, key)))) { // EQUIVALENCE_MOD
                           V prevVal = e.val;
                           prevVal = prevVal == NULL_VALUE ? null : prevVal;
                           val = remappingFunction.apply(prevVal, value);
                           if (val != null) {
                              delta = prevVal == null ? 1 : 0;
                              e.val = val;
                           }
                           else {
                              delta = prevVal == null ? 0 : -1;
                              Node<K,V> en = e.next;
                              if (pred != null)
                                 pred.next = en;
                              else
                                 setTabAt(tab, i, en);
                           }
                           break;
                        }
                        pred = e;
                        if ((e = e.next) == null) {
                           delta = 1;
                           val = value;
                           pred.next = evictionPolicy.createNewEntry(key, h, null, val,
                                 null);
                           evictionPolicy.onEntryMiss(pred.next, val);
                           evictionListener.onEntryActivated(key);
                           break;
                        }
                     }
                  }
                  else if (f instanceof TreeBin) {
                     binCount = 2;
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> r = t.root;
                     TreeNode<K,V> p = (r == null) ? null :
                           r.findTreeNode(h, key, null);
                     V prevVal = p.val;
                     prevVal = prevVal == NULL_VALUE ? null : prevVal;
                     val = (p == null) ? value :
                           remappingFunction.apply(prevVal, value);
                     if (val != null) {
                        if (p != null) {
                           // If val was null then this is an add
                           delta = prevVal == null ? 1 : 0;
                           p.val = val;
                        }
                        else {
                           delta = 1;
                           t.putTreeVal(h, key, val);
                        }
                     }
                     else if (p != null) {
                        // If val was null then it wasn't removed - but we still remove
                        // the node
                        delta = prevVal == null ? 0 : -1;
                        if (t.removeTreeNode(p))
                           setTabAt(tab, i, untreeify(t.first)); // EQUIVALENCE_MOD
                     }
                  }
               }
            }
            if (binCount != 0) {
               if (binCount >= TREEIFY_THRESHOLD)
                  treeifyBin(tab, i);
               break;
            }
         }
      }
      if (delta != 0) {
         addCount((long)delta, binCount);
         notifyEvictionListener(evictionPolicy.findIfEntriesNeedEvicting());
      }
      return val;
   }

   // Hashtable legacy methods

   /**
    * Legacy method testing if some key maps into the specified value
    * in this table.  This method is identical in functionality to
    * {@link #containsValue(Object)}, and exists solely to ensure
    * full compatibility with class {@link java.util.Hashtable},
    * which supported this method prior to introduction of the
    * Java Collections framework.
    *
    * @param  value a value to search for
    * @return {@code true} if and only if some key maps to the
    *         {@code value} argument in this table as
    *         determined by the {@code equals} method;
    *         {@code false} otherwise
    * @throws NullPointerException if the specified value is null
    */
   @Deprecated public boolean contains(Object value) {
      return containsValue(value);
   }

   /**
    * Returns an enumeration of the keys in this table.
    *
    * @return an enumeration of the keys in this table
    * @see #keySet()
    */
   public Enumeration<K> keys() {
      Node<K,V>[] t;
      int f = (t = table) == null ? 0 : t.length;
      return new KeyIterator<K,V>(t, f, 0, f, this);
   }

   /**
    * Returns an enumeration of the values in this table.
    *
    * @return an enumeration of the values in this table
    * @see #values()
    */
   public Enumeration<V> elements() {
      Node<K,V>[] t;
      int f = (t = table) == null ? 0 : t.length;
      return new ValueIterator<K,V>(t, f, 0, f, this);
   }

   // EquivalentConcurrentHashMapV8-only methods

   /**
    * Returns the number of mappings. This method should be used
    * instead of {@link #size} because a EquivalentConcurrentHashMapV8 may
    * contain more mappings than can be represented as an int. The
    * value returned is an estimate; the actual count may differ if
    * there are concurrent insertions or removals.
    *
    * @return the number of mappings
    * @since 1.8
    */
   public long mappingCount() {
      long n = sumCount();
      return (n < 0L) ? 0L : n; // ignore transient negative values
   }

   /**
    * Creates a new {@link Set} backed by a EquivalentConcurrentHashMapV8
    * from the given type to {@code Boolean.TRUE}.
    *
    * @return the new set
    * @since 1.8
    */
   public static <K> KeySetView<K,Boolean> newKeySet(int maxSize, Equivalence<K> keyEquivalence) { // EQUIVALENCE_MOD
      return new KeySetView<K,Boolean>
            (new BoundedEquivalentConcurrentHashMapV8<K,Boolean>(maxSize, keyEquivalence,
                  AnyEquivalence.<Boolean>getInstance()), Boolean.TRUE); // EQUIVALENCE_MOD
   }

   /**
    * Creates a new {@link Set} backed by a EquivalentConcurrentHashMapV8
    * from the given type to {@code Boolean.TRUE}.
    *
    * @param initialCapacity The implementation performs internal
    * sizing to accommodate this many elements.
    * @return the new set
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative
    * @since 1.8
    */
   public static <K> KeySetView<K,Boolean> newKeySet(int maxSize, int initialCapacity, Equivalence<K> keyEquivalence) { // EQUIVALENCE_MOD
      return new KeySetView<K,Boolean>
            (new BoundedEquivalentConcurrentHashMapV8<K,Boolean>(maxSize, initialCapacity, keyEquivalence,
                  AnyEquivalence.<Boolean>getInstance()), Boolean.TRUE); // EQUIVALENCE_MOD
   }

   /**
    * Returns a {@link Set} view of the keys in this map, using the
    * given common mapped value for any additions (i.e., {@link
    * Collection#add} and {@link Collection#addAll(Collection)}).
    * This is of course only appropriate if it is acceptable to use
    * the same value for all additions from this view.
    *
    * @param mappedValue the mapped value to use for any additions
    * @return the set view
    * @throws NullPointerException if the mappedValue is null
    */
   public KeySetView<K,V> keySet(V mappedValue) {
      if (mappedValue == null)
         throw new NullPointerException();
      return new KeySetView<K,V>(this, mappedValue);
   }

    /* ---------------- Special Nodes -------------- */

   /**
    * A node inserted at head of bins during transfer operations.
    */
   static final class ForwardingNode<K,V> extends Node<K,V> {
      final Node<K,V>[] nextTable;
      ForwardingNode(Node<K,V>[] tab, NodeEquivalence<K, V> nodeEq) { // EQUIVALENCE_MOD
         super(MOVED, nodeEq, null, null, null); // EQUIVALENCE_MOD
         this.nextTable = tab;
      }

      Node<K,V> find(int h, Object k) {
         // loop to avoid arbitrarily deep recursion on forwarding nodes
         outer: for (Node<K,V>[] tab = nextTable;;) {
            Node<K,V> e; int n;
            if (k == null || tab == null || (n = tab.length) == 0 ||
                  (e = tabAt(tab, (n - 1) & h)) == null)
               return null;
            for (;;) {
               int eh; K ek;
               if ((eh = e.hash) == h &&
                     ((ek = e.key) == k || (ek != null && nodeEq.keyEq.equals(ek, k)))) // EQUIVALENCE_MOD
                  return e;
               if (eh < 0) {
                  if (e instanceof ForwardingNode) {
                     tab = ((ForwardingNode<K,V>)e).nextTable;
                     continue outer;
                  }
                  else
                     return e.find(h, k);
               }
               if ((e = e.next) == null)
                  return null;
            }
         }
      }
   }

   /**
    * A place-holder node used in computeIfAbsent and compute
    */
   static final class ReservationNode<K,V> extends Node<K,V> {
      ReservationNode(NodeEquivalence<K, V> nodeEq) { // EQUIVALENCE_MOD
         super(RESERVED, nodeEq, null, null, null); // EQUIVALENCE_MOD
      }

      Node<K,V> find(int h, Object k) {
         return null;
      }
   }

    /* ---------------- Table Initialization and Resizing -------------- */

   /**
    * Returns the stamp bits for resizing a table of size n.
    * Must be negative when shifted left by RESIZE_STAMP_SHIFT.
    */
   static final int resizeStamp(int n) {
      return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
   }

   /**
    * Initializes table, using the size recorded in sizeCtl.
    */
   private final Node<K,V>[] initTable() {
      Node<K,V>[] tab; int sc;
      while ((tab = table) == null || tab.length == 0) {
         if ((sc = sizeCtl) < 0)
            Thread.yield(); // lost initialization race; just spin
         else if (U.compareAndSwapInt(this, SIZECTL, sc, -1)) {
            try {
               if ((tab = table) == null || tab.length == 0) {
                  int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                  @SuppressWarnings("unchecked")
                  Node<K,V>[] nt = (Node<K,V>[])new Node<?,?>[n];
                  table = tab = nt;
                  sc = n - (n >>> 2);
                  evictionPolicy.onResize(0, n);
               }
            } finally {
               sizeCtl = sc;
            }
            break;
         }
      }
      return tab;
   }

   /**
    * Adds to count, and if table is too small and not already
    * resizing, initiates transfer. If already resizing, helps
    * perform transfer if work is available.  Rechecks occupancy
    * after a transfer to see if another resize is already needed
    * because resizings are lagging additions.
    *
    * @param x the count to add
    * @param check if <0, don't check resize, if <= 1 only check if uncontended
    */
   private final void addCount(long x, int check) {
      CounterCell[] as; long b, s;
      if ((as = counterCells) != null ||
            !U.compareAndSwapLong(this, BASECOUNT, b = baseCount, s = b + x)) {
         CounterHashCode hc; CounterCell a; long v; int m;
         boolean uncontended = true;
         if ((hc = threadCounterHashCode.get()) == null ||
               as == null || (m = as.length - 1) < 0 ||
               (a = as[m & hc.code]) == null ||
               !(uncontended =
                       U.compareAndSwapLong(a, CELLVALUE, v = a.value, v + x))) {
            fullAddCount(x, hc, uncontended);
            return;
         }
         // If this is true it means an element was removed or the element wasn't replaced
         // In either case we don't care about the size
         if (check <= -1)
            return;
         s = sumCount();
      }
      if (check >= 0) {
         Node<K,V>[] tab, nt; int n, sc;
         while (s >= (long)(sc = sizeCtl) && (sizeCtl * .75) < maxSize && (tab = table) != null &&
               (n = tab.length) < MAXIMUM_CAPACITY) {
            int rs = resizeStamp(n);
            if (sc < 0) {
               if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                     sc == rs + MAX_RESIZERS || (nt = nextTable) == null ||
                     transferIndex <= 0)
                  break;
               if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1))
                  transfer(tab, nt);
            }
            else if (U.compareAndSwapInt(this, SIZECTL, sc,
                  (rs << RESIZE_STAMP_SHIFT) + 2))
               transfer(tab, null);
            s = sumCount();
         }
      }
      return;
   }

   /**
    * Helps transfer if a resize is in progress.
    */
   final Node<K,V>[] helpTransfer(Node<K,V>[] tab, Node<K,V> f) {
      Node<K,V>[] nextTab; int sc;
      if (tab != null && (f instanceof ForwardingNode) &&
            (nextTab = ((ForwardingNode<K,V>)f).nextTable) != null) {
         int rs = resizeStamp(tab.length);
         while (nextTab == nextTable && table == tab &&
               (sc = sizeCtl) < 0) {
            if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                  sc == rs + MAX_RESIZERS || transferIndex <= 0)
               break;
            if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
               transfer(tab, nextTab);
               break;
            }
         }
         return nextTab;
      }
      return table;
   }

   /**
    * Tries to presize table to accommodate the given number of elements.
    *
    * @param size number of elements (doesn't need to be perfectly accurate)
    */
   private final void tryPresize(int size) {
      int c = (size >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
            tableSizeFor(size + (size >>> 1) + 1);
      int sc;
      while ((sc = sizeCtl) >= 0) {
         Node<K,V>[] tab = table; int n;
         if (tab == null || (n = tab.length) == 0) {
            n = (sc > c) ? sc : c;
            if (U.compareAndSwapInt(this, SIZECTL, sc, -1)) {
               try {
                  if (table == tab) {
                     @SuppressWarnings("unchecked")
                     Node<K,V>[] nt = (Node<K,V>[])new Node<?,?>[n];
                     table = nt;
                     sc = n - (n >>> 2);
                     evictionPolicy.onResize(tab.length, n);
                  }
               } finally {
                  sizeCtl = sc;
               }
            }
         }
         else if (c <= sc || n >= MAXIMUM_CAPACITY)
            break;
         else if (tab == table) {
            int rs = resizeStamp(n);
            if (sc < 0) {
               Node<K,V>[] nt;
               if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                     sc == rs + MAX_RESIZERS || (nt = nextTable) == null ||
                     transferIndex <= 0)
                  break;
               if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1))
                  transfer(tab, nt);
            }
            else if (U.compareAndSwapInt(this, SIZECTL, sc,
                  (rs << RESIZE_STAMP_SHIFT) + 2))
               transfer(tab, null);
         }
      }
   }

   /**
    * Moves and/or copies the nodes in each bin to new table. See
    * above for explanation.
    */
   private final void transfer(Node<K,V>[] tab, Node<K,V>[] nextTab) {
      int n = tab.length, stride;
      if ((stride = (NCPU > 1) ? (n >>> 3) / NCPU : n) < MIN_TRANSFER_STRIDE)
         stride = MIN_TRANSFER_STRIDE; // subdivide range
      if (nextTab == null) {            // initiating
         try {
            @SuppressWarnings("unchecked")
            Node<K,V>[] nt = (Node<K,V>[])new Node<?,?>[n << 1];
            nextTab = nt;
         } catch (Throwable ex) {      // try to cope with OOME
            sizeCtl = Integer.MAX_VALUE;
            return;
         }
         nextTable = nextTab;
         transferIndex = n;
      }
      int nextn = nextTab.length;
      ForwardingNode<K,V> fwd = new ForwardingNode<K,V>(nextTab, nodeEq); // EQUIVALENCE_MOD
      boolean advance = true;
      boolean finishing = false; // to ensure sweep before committing nextTab
      for (int i = 0, bound = 0;;) {
         Node<K,V> f; int fh;
         while (advance) {
            int nextIndex, nextBound;
            if (--i >= bound || finishing)
               advance = false;
            else if ((nextIndex = transferIndex) <= 0) {
               i = -1;
               advance = false;
            }
            else if (U.compareAndSwapInt
                  (this, TRANSFERINDEX, nextIndex,
                        nextBound = (nextIndex > stride ?
                                           nextIndex - stride : 0))) {
               bound = nextBound;
               i = nextIndex - 1;
               advance = false;
            }
         }
         if (i < 0 || i >= n || i + n >= nextn) {
            int sc;
            if (finishing) {
               nextTable = null;
               long oldSize = table.length;
               table = nextTab;
               sizeCtl = (n << 1) - (n >>> 1);
               evictionPolicy.onResize(oldSize, table.length);
               return;
            }
            if (U.compareAndSwapInt(this, SIZECTL, sc = sizeCtl, sc - 1)) {
               if ((sc - 2) != resizeStamp(n) << RESIZE_STAMP_SHIFT)
                  return;
               finishing = advance = true;
               i = n; // recheck before commit
            }
         }
         else if ((f = tabAt(tab, i)) == null)
            advance = casTabAt(tab, i, null, fwd);
         else if ((fh = f.hash) == MOVED)
            advance = true; // already processed
         else {
            synchronized (f) {
               if (tabAt(tab, i) == f) {
                  Node<K,V> ln, hn;
                  if (fh >= 0) {
                     int runBit = fh & n;
                     Node<K,V> lastRun = f;
                     for (Node<K,V> p = f.next; p != null; p = p.next) {
                        int b = p.hash & n;
                        if (b != runBit) {
                           runBit = b;
                           lastRun = p;
                        }
                     }
                     if (runBit == 0) {
                        ln = lastRun;
                        hn = null;
                     }
                     else {
                        hn = lastRun;
                        ln = null;
                     }
                     for (Node<K,V> p = f; p != lastRun; p = p.next) {
                        int ph = p.hash; K pk = p.key; V pv = p.val;
                        if ((ph & n) == 0) {
                           ln = evictionPolicy.createNewEntry(pk, ph, ln, pv, p.eviction);
                        }
                        else {
                           hn = evictionPolicy.createNewEntry(pk, ph, hn, pv, p.eviction);
                        }
                     }
                     setTabAt(nextTab, i, ln);
                     setTabAt(nextTab, i + n, hn);
                     setTabAt(tab, i, fwd);
                     advance = true;
                  }
                  else if (f instanceof TreeBin) {
                     TreeBin<K,V> t = (TreeBin<K,V>)f;
                     TreeNode<K,V> lo = null, loTail = null;
                     TreeNode<K,V> hi = null, hiTail = null;
                     int lc = 0, hc = 0;
                     for (Node<K,V> e = t.first; e != null; e = e.next) {
                        int h = e.hash;
                        TreeNode<K,V> p = evictionPolicy.createNewEntry(e.key, h, null, 
                              null, e.val, e.eviction);
                        if ((h & n) == 0) {
                           if ((p.prev = loTail) == null)
                              lo = p;
                           else
                              loTail.next = p;
                           loTail = p;
                           ++lc;
                        }
                        else {
                           if ((p.prev = hiTail) == null)
                              hi = p;
                           else
                              hiTail.next = p;
                           hiTail = p;
                           ++hc;
                        }
                     }
                     ln = (lc <= UNTREEIFY_THRESHOLD) ? untreeify(lo) : // EQUIVALENCE_MOD
                           (hc != 0) ? new TreeBin<K,V>(lo, this) : t; // EQUIVALENCE_MOD
                     hn = (hc <= UNTREEIFY_THRESHOLD) ? untreeify(hi) : // EQUIVALENCE_MOD
                           (lc != 0) ? new TreeBin<K,V>(hi, this) : t; // EQUIVALENCE_MOD
                     setTabAt(nextTab, i, ln);
                     setTabAt(nextTab, i + n, hn);
                     setTabAt(tab, i, fwd);
                     advance = true;
                  }
               }
            }
         }
      }
   }

    /* ---------------- Conversion from/to TreeBins -------------- */

   /**
    * Replaces all linked nodes in bin at given index unless table is
    * too small, in which case resizes instead.
    */
   private final void treeifyBin(Node<K,V>[] tab, int index) {
      Node<K,V> b; int n;
      if (tab != null) {
         if ((n = tab.length) < MIN_TREEIFY_CAPACITY)
            tryPresize(n << 1);
         else if ((b = tabAt(tab, index)) != null && b.hash >= 0) {
            synchronized (b) {
               if (tabAt(tab, index) == b) {
                  TreeNode<K,V> hd = null, tl = null;
                  for (Node<K,V> e = b; e != null; e = e.next) {
                     TreeNode<K,V> p = evictionPolicy.createNewEntry(e.key, e.hash, null,
                           null, e.val, e.eviction);
                     if ((p.prev = tl) == null)
                        hd = p;
                     else
                        tl.next = p;
                     tl = p;
                  }
                  setTabAt(tab, index, new TreeBin<K,V>(hd, this));
               }
            }
         }
      }
   }

   /**
    * Returns a list on non-TreeNodes replacing those in given list.
    */
   Node<K,V> untreeify(Node<K,V> b) { // EQUIVALENCE_MOD
      Node<K,V> hd = null, tl = null;
      for (Node<K,V> q = b; q != null; q = q.next) {
         Node<K,V> p = evictionPolicy.createNewEntry(q.key, q.hash, null, q.val,
               q.eviction);
         if (tl == null)
            hd = p;
         else
            tl.next = p;
         tl = p;
      }
      return hd;
   }

    /* ---------------- TreeNodes -------------- */

   /**
    * Nodes for use in TreeBins
    */
   static final class TreeNode<K,V> extends Node<K,V> {
      TreeNode<K,V> parent;  // red-black tree links
      TreeNode<K,V> left;
      TreeNode<K,V> right;
      TreeNode<K,V> prev;    // needed to unlink next upon deletion
      boolean red;
      
      TreeNode(int hash, NodeEquivalence<K,V> nodeEq, K key, V val, Node<K,V> next, // EQUIVALENCE_MOD
            TreeNode<K,V> parent, EvictionEntry<K, V> evictionEntry) {
         super(hash, nodeEq, key, val, next); // EQUIVALENCE_MOD
         this.parent = parent;
         if (evictionEntry != null) {
            lazySetEviction(evictionEntry);
         }
      }

      Node<K,V> find(int h, Object k) {
         return findTreeNode(h, k, null);
      }

      /**
       * Returns the TreeNode (or null if not found) for the given key
       * starting at given root.
       */
      @SuppressWarnings("unchecked")
      final TreeNode<K,V> findTreeNode(int h, Object k, Class<?> kc) {
         if (k != null) {
            TreeNode<K,V> p = this;
            do  {
               int ph, dir; K pk; TreeNode<K,V> q;
               TreeNode<K,V> pl = p.left, pr = p.right;
               if ((ph = p.hash) > h)
                  p = pl;
               else if (ph < h)
                  p = pr;
               else if ((pk = p.key) == k || (pk != null && nodeEq.keyEq.equals(pk, k))) // EQUIVALENCE_MOD
                  return p;
               else if (pl == null)
                  p = pr;
               else if (pr == null)
                  p = pl;
               else if ((kc != null ||
                               (kc = comparableClassFor(k, nodeEq.keyEq)) != null) && // EQUIVALENCE_MOD
                     (dir = compareComparables(kc, k, pk, (Equivalence<Object>) nodeEq.keyEq)) != 0) // EQUIVALENCE_MOD
                  p = (dir < 0) ? pl : pr;
               else if ((q = pr.findTreeNode(h, k, kc)) != null)
                  return q;
               else
                  p = pl;
            } while (p != null);
         }
         return null;
      }

      public String toString() {
         return "Tree" + super.toString() + " with hash " + System.identityHashCode(this);
      }
   }

    /* ---------------- TreeBins -------------- */

   /**
    * TreeNodes used at the heads of bins. TreeBins do not hold user
    * keys or values, but instead point to list of TreeNodes and
    * their root. They also maintain a parasitic read-write lock
    * forcing writers (who hold bin lock) to wait for readers (who do
    * not) to complete before tree restructuring operations.
    */
   static final class TreeBin<K,V> extends Node<K,V> {
      final BoundedEquivalentConcurrentHashMapV8<K, V> map;
      TreeNode<K,V> root;
      volatile TreeNode<K,V> first;
      volatile Thread waiter;
      volatile int lockState;
      // values for lockState
      static final int WRITER = 1; // set while holding write lock
      static final int WAITER = 2; // set when waiting for write lock
      static final int READER = 4; // increment value for setting read lock

      /**
       * Tie-breaking utility for ordering insertions when equal
       * hashCodes and non-comparable. We don't require a total
       * order, just a consistent insertion rule to maintain
       * equivalence across rebalancings. Tie-breaking further than
       * necessary simplifies testing a bit.
       */
      static int tieBreakOrder(Object a, Object b) {
         int d;
         if (a == null || b == null ||
               (d = a.getClass().getName().
                     compareTo(b.getClass().getName())) == 0)
            d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
                       -1 : 1);
         return d;
      }

      /**
       * Creates bin with initial set of nodes headed by b.
       */
      @SuppressWarnings("unchecked")
      TreeBin(TreeNode<K,V> b, BoundedEquivalentConcurrentHashMapV8<K, V> map) { // EQUIVALENCE_MOD
         super(TREEBIN, map.nodeEq, null, null, null);
         this.map = map;
         this.first = b;
         TreeNode<K,V> r = null;
         for (TreeNode<K,V> x = b, next; x != null; x = next) {
            next = (TreeNode<K,V>)x.next;
            x.left = x.right = null;
            if (r == null) {
               x.parent = null;
               x.red = false;
               r = x;
            }
            else {
               K k = x.key;
               int h = x.hash;
               Class<?> kc = null;
               for (TreeNode<K,V> p = r;;) {
                  int dir, ph;
                  K pk = p.key;
                  if ((ph = p.hash) > h)
                     dir = -1;
                  else if (ph < h)
                     dir = 1;
                  else if ((kc == null &&
                        (kc = comparableClassFor(k, nodeEq.keyEq)) == null) || // EQUIVALENCE_MOD
                        (dir = compareComparables(kc, k, pk, (Equivalence<Object>) nodeEq.keyEq)) == 0) // EQUIVALENCE_MOD
                     dir = tieBreakOrder(k, pk);

                  TreeNode<K,V> xp = p;
                  if ((p = (dir <= 0) ? p.left : p.right) == null) {
                     x.parent = xp;
                     if (dir <= 0)
                        xp.left = x;
                     else
                        xp.right = x;
                     r = balanceInsertion(r, x);
                     break;
                  }
               }
            }
         }
         this.root = r;
//         assert checkInvariants(root);
      }

      /**
       * Acquires write lock for tree restructuring.
       */
      private final void lockRoot() {
         if (!U.compareAndSwapInt(this, LOCKSTATE, 0, WRITER))
            contendedLock(); // offload to separate method
      }

      /**
       * Releases write lock for tree restructuring.
       */
      private final void unlockRoot() {
         lockState = 0;
      }

      /**
       * Possibly blocks awaiting root lock.
       */
      private final void contendedLock() {
         boolean waiting = false;
         for (int s;;) {
            if (((s = lockState) & ~WAITER) == 0) {
               if (U.compareAndSwapInt(this, LOCKSTATE, s, WRITER)) {
                  if (waiting)
                     waiter = null;
                  return;
               }
            }
            else if ((s & WAITER) == 0) {
               if (U.compareAndSwapInt(this, LOCKSTATE, s, s | WAITER)) {
                  waiting = true;
                  waiter = Thread.currentThread();
               }
            }
            else if (waiting)
               LockSupport.park(this);
         }
      }

      /**
       * Returns matching node or null if none. Tries to search
       * using tree comparisons from root, but continues linear
       * search when lock not available.
       */
      final Node<K,V> find(int h, Object k) {
         if (k != null) {
            for (Node<K,V> e = first; e != null; ) {
               int s; K ek;
               if (((s = lockState) & (WAITER|WRITER)) != 0) {
                  if (e.hash == h &&
                        ((ek = e.key) == k || (ek != null && nodeEq.keyEq.equals(ek, k)))) // EQUIVALENCE_MOD
                     return e;
                  e = e.next;
               }
               else if (U.compareAndSwapInt(this, LOCKSTATE, s,
                     s + READER)) {
                  TreeNode<K,V> r, p;
                  try {
                     p = ((r = root) == null ? null :
                                r.findTreeNode(h, k, null));
                  } finally {
                     Thread w;
                     int ls;
                     do {} while (!U.compareAndSwapInt
                           (this, LOCKSTATE,
                                 ls = lockState, ls - READER));
                     if (ls == (READER|WAITER) && (w = waiter) != null)
                        LockSupport.unpark(w);
                  }
                  return p;
               }
            }
         }
         return null;
      }

      /**
       * Finds or adds a node.
       * @return null if added
       */
      @SuppressWarnings("unchecked")
      final TreeNode<K,V> putTreeVal(int h, K k, V v) {
         Class<?> kc = null;
         boolean searched = false;
         for (TreeNode<K,V> p = root;;) {
            int dir, ph; K pk;
            if (p == null) {
               first = root = map.evictionPolicy.createNewEntry(k, h, null, null, v, null);
               map.evictionPolicy.onEntryMiss(first, v);
               map.evictionListener.onEntryActivated(k);
               break;
            }
            else if ((ph = p.hash) > h)
               dir = -1;
            else if (ph < h)
               dir = 1;
            else if ((pk = p.key) == k || (pk != null && nodeEq.keyEq.equals(pk, k))) // EQUIVALENCE_MOD
               return p;
            else if ((kc == null &&
                     (kc = comparableClassFor(k, nodeEq.keyEq)) == null) || // EQUIVALENCE_MOD
                     (dir = compareComparables(kc, k, pk, (Equivalence<Object>) nodeEq.keyEq)) == 0) { // EQUIVALENCE_MOD
               if (!searched) {
                  TreeNode<K,V> q, ch;
                  searched = true;
                  if (((ch = p.left) != null &&
                     (q = ch.findTreeNode(h, k, kc)) != null) ||
                     ((ch = p.right) != null &&
                     (q = ch.findTreeNode(h, k, kc)) != null))
                  return q;
               }
               dir = tieBreakOrder(k, pk);
            }
            TreeNode<K,V> xp = p;
            if ((p = (dir <= 0) ? p.left : p.right) == null) {
               TreeNode<K,V> x, f = first;
               first = x = map.evictionPolicy.createNewEntry(k, h, f, xp, v, null);
               map.evictionPolicy.onEntryMiss(first, v);
               map.evictionListener.onEntryActivated(k);
               if (f != null)
                  f.prev = x;
               if (dir <= 0)
                  xp.left = x;
               else
                  xp.right = x;
               if (!xp.red)
                  x.red = true;
               else {
                  lockRoot();
                  try {
                     root = balanceInsertion(root, x);
                  } finally {
                     unlockRoot();
                  }
               }
               break;
            }
         }
         assert checkInvariants(root);
         return null;
      }

      /**
       * Removes the given node, that must be present before this
       * call.  This is messier than typical red-black deletion code
       * because we cannot swap the contents of an interior node
       * with a leaf successor that is pinned by "next" pointers
       * that are accessible independently of lock. So instead we
       * swap the tree linkages.
       *
       * @return true if now too small, so should be untreeified
       */
      final boolean removeTreeNode(TreeNode<K,V> p) {
         TreeNode<K,V> next = (TreeNode<K,V>)p.next;
         TreeNode<K,V> pred = p.prev;  // unlink traversal pointers
         TreeNode<K,V> r, rl;
         if (pred == null)
            first = next;
         else
            pred.next = next;
         if (next != null)
            next.prev = pred;
         if (first == null) {
            root = null;
            return true;
         }
         if ((r = root) == null || r.right == null || // too small
               (rl = r.left) == null || rl.left == null)
            return true;
         lockRoot();
         try {
            TreeNode<K,V> replacement;
            TreeNode<K,V> pl = p.left;
            TreeNode<K,V> pr = p.right;
            if (pl != null && pr != null) {
               TreeNode<K,V> s = pr, sl;
               while ((sl = s.left) != null) // find successor
                  s = sl;
               boolean c = s.red; s.red = p.red; p.red = c; // swap colors
               TreeNode<K,V> sr = s.right;
               TreeNode<K,V> pp = p.parent;
               if (s == pr) { // p was s's direct parent
                  p.parent = s;
                  s.right = p;
               }
               else {
                  TreeNode<K,V> sp = s.parent;
                  if ((p.parent = sp) != null) {
                     if (s == sp.left)
                        sp.left = p;
                     else
                        sp.right = p;
                  }
                  if ((s.right = pr) != null)
                     pr.parent = s;
               }
               p.left = null;
               if ((p.right = sr) != null)
                  sr.parent = p;
               if ((s.left = pl) != null)
                  pl.parent = s;
               if ((s.parent = pp) == null)
                  r = s;
               else if (p == pp.left)
                  pp.left = s;
               else
                  pp.right = s;
               if (sr != null)
                  replacement = sr;
               else
                  replacement = p;
            }
            else if (pl != null)
               replacement = pl;
            else if (pr != null)
               replacement = pr;
            else
               replacement = p;
            if (replacement != p) {
               TreeNode<K,V> pp = replacement.parent = p.parent;
               if (pp == null)
                  r = replacement;
               else if (p == pp.left)
                  pp.left = replacement;
               else
                  pp.right = replacement;
               p.left = p.right = p.parent = null;
            }

            root = (p.red) ? r : balanceDeletion(r, replacement);

            if (p == replacement) {  // detach pointers
               TreeNode<K,V> pp;
               if ((pp = p.parent) != null) {
                  if (p == pp.left)
                     pp.left = null;
                  else if (p == pp.right)
                     pp.right = null;
                  p.parent = null;
               }
            }
         } finally {
            unlockRoot();
         }
//         assert checkInvariants(root);
         return false;
      }

        /* ------------------------------------------------------------ */
      // Red-black tree methods, all adapted from CLR

      static <K,V> TreeNode<K,V> rotateLeft(TreeNode<K,V> root,
            TreeNode<K,V> p) {
         TreeNode<K,V> r, pp, rl;
         if (p != null && (r = p.right) != null) {
            if ((rl = p.right = r.left) != null)
               rl.parent = p;
            if ((pp = r.parent = p.parent) == null)
               (root = r).red = false;
            else if (pp.left == p)
               pp.left = r;
            else
               pp.right = r;
            r.left = p;
            p.parent = r;
         }
         return root;
      }

      static <K,V> TreeNode<K,V> rotateRight(TreeNode<K,V> root,
            TreeNode<K,V> p) {
         TreeNode<K,V> l, pp, lr;
         if (p != null && (l = p.left) != null) {
            if ((lr = p.left = l.right) != null)
               lr.parent = p;
            if ((pp = l.parent = p.parent) == null)
               (root = l).red = false;
            else if (pp.right == p)
               pp.right = l;
            else
               pp.left = l;
            l.right = p;
            p.parent = l;
         }
         return root;
      }

      static <K,V> TreeNode<K,V> balanceInsertion(TreeNode<K,V> root,
            TreeNode<K,V> x) {
         x.red = true;
         for (TreeNode<K,V> xp, xpp, xppl, xppr;;) {
            if ((xp = x.parent) == null) {
               x.red = false;
               return x;
            }
            else if (!xp.red || (xpp = xp.parent) == null)
               return root;
            if (xp == (xppl = xpp.left)) {
               if ((xppr = xpp.right) != null && xppr.red) {
                  xppr.red = false;
                  xp.red = false;
                  xpp.red = true;
                  x = xpp;
               }
               else {
                  if (x == xp.right) {
                     root = rotateLeft(root, x = xp);
                     xpp = (xp = x.parent) == null ? null : xp.parent;
                  }
                  if (xp != null) {
                     xp.red = false;
                     if (xpp != null) {
                        xpp.red = true;
                        root = rotateRight(root, xpp);
                     }
                  }
               }
            }
            else {
               if (xppl != null && xppl.red) {
                  xppl.red = false;
                  xp.red = false;
                  xpp.red = true;
                  x = xpp;
               }
               else {
                  if (x == xp.left) {
                     root = rotateRight(root, x = xp);
                     xpp = (xp = x.parent) == null ? null : xp.parent;
                  }
                  if (xp != null) {
                     xp.red = false;
                     if (xpp != null) {
                        xpp.red = true;
                        root = rotateLeft(root, xpp);
                     }
                  }
               }
            }
         }
      }

      static <K,V> TreeNode<K,V> balanceDeletion(TreeNode<K,V> root,
            TreeNode<K,V> x) {
         for (TreeNode<K,V> xp, xpl, xpr;;)  {
            if (x == null || x == root)
               return root;
            else if ((xp = x.parent) == null) {
               x.red = false;
               return x;
            }
            else if (x.red) {
               x.red = false;
               return root;
            }
            else if ((xpl = xp.left) == x) {
               if ((xpr = xp.right) != null && xpr.red) {
                  xpr.red = false;
                  xp.red = true;
                  root = rotateLeft(root, xp);
                  xpr = (xp = x.parent) == null ? null : xp.right;
               }
               if (xpr == null)
                  x = xp;
               else {
                  TreeNode<K,V> sl = xpr.left, sr = xpr.right;
                  if ((sr == null || !sr.red) &&
                        (sl == null || !sl.red)) {
                     xpr.red = true;
                     x = xp;
                  }
                  else {
                     if (sr == null || !sr.red) {
                        if (sl != null)
                           sl.red = false;
                        xpr.red = true;
                        root = rotateRight(root, xpr);
                        xpr = (xp = x.parent) == null ?
                              null : xp.right;
                     }
                     if (xpr != null) {
                        xpr.red = (xp == null) ? false : xp.red;
                        if ((sr = xpr.right) != null)
                           sr.red = false;
                     }
                     if (xp != null) {
                        xp.red = false;
                        root = rotateLeft(root, xp);
                     }
                     x = root;
                  }
               }
            }
            else { // symmetric
               if (xpl != null && xpl.red) {
                  xpl.red = false;
                  xp.red = true;
                  root = rotateRight(root, xp);
                  xpl = (xp = x.parent) == null ? null : xp.left;
               }
               if (xpl == null)
                  x = xp;
               else {
                  TreeNode<K,V> sl = xpl.left, sr = xpl.right;
                  if ((sl == null || !sl.red) &&
                        (sr == null || !sr.red)) {
                     xpl.red = true;
                     x = xp;
                  }
                  else {
                     if (sl == null || !sl.red) {
                        if (sr != null)
                           sr.red = false;
                        xpl.red = true;
                        root = rotateLeft(root, xpl);
                        xpl = (xp = x.parent) == null ?
                              null : xp.left;
                     }
                     if (xpl != null) {
                        xpl.red = (xp == null) ? false : xp.red;
                        if ((sl = xpl.left) != null)
                           sl.red = false;
                     }
                     if (xp != null) {
                        xp.red = false;
                        root = rotateRight(root, xp);
                     }
                     x = root;
                  }
               }
            }
         }
      }

      /**
       * Recursive invariant check
       */
      static <K,V> boolean checkInvariants(TreeNode<K,V> t) {
         TreeNode<K,V> tp = t.parent, tl = t.left, tr = t.right,
               tb = t.prev, tn = (TreeNode<K,V>)t.next;
         if (tb != null && tb.next != t)
            return false;
         if (tn != null && tn.prev != t)
            return false;
         if (tp != null && t != tp.left && t != tp.right)
            return false;
         if (tl != null && (tl.parent != t || tl.hash > t.hash))
            return false;
         if (tr != null && (tr.parent != t || tr.hash < t.hash))
            return false;
         if (t.red && tl != null && tl.red && tr != null && tr.red)
            return false;
         if (tl != null && !checkInvariants(tl))
            return false;
         if (tr != null && !checkInvariants(tr))
            return false;
         return true;
      }

      private static final sun.misc.Unsafe U;
      private static final long LOCKSTATE;
      static {
         try {
            U = getUnsafe();
            Class<?> k = TreeBin.class;
            LOCKSTATE = U.objectFieldOffset
                  (k.getDeclaredField("lockState"));
         } catch (Exception e) {
            throw new Error(e);
         }
      }
   }

    /* ----------------Table Traversal -------------- */

   /**
    * Records the table, its length, and current traversal index for a
    * traverser that must process a region of a forwarded table before
    * proceeding with current table.
    */
   static final class TableStack<K,V> {
      int length;
      int index;
      Node<K,V>[] tab;
      TableStack<K,V> next;
   }

   /**
    * Encapsulates traversal for methods such as containsValue; also
    * serves as a base class for other iterators and spliterators.
    *
    * Method advance visits once each still-valid node that was
    * reachable upon iterator construction. It might miss some that
    * were added to a bin after the bin was visited, which is OK wrt
    * consistency guarantees. Maintaining this property in the face
    * of possible ongoing resizes requires a fair amount of
    * bookkeeping state that is difficult to optimize away amidst
    * volatile accesses.  Even so, traversal maintains reasonable
    * throughput.
    *
    * Normally, iteration proceeds bin-by-bin traversing lists.
    * However, if the table has been resized, then all future steps
    * must traverse both the bin at the current index as well as at
    * (index + baseSize); and so on for further resizings. To
    * paranoically cope with potential sharing by users of iterators
    * across threads, iteration terminates if a bounds checks fails
    * for a table read.
    */
   static class Traverser<K,V> {
      Node<K,V>[] tab;        // current table; updated if resized
      Node<K,V> next;         // the next entry to use
      TableStack<K,V> stack, spare; // to save/restore on ForwardingNodes
      int index;              // index of bin to use next
      int baseIndex;          // current index of initial table
      int baseLimit;          // index bound for initial table
      final int baseSize;     // initial table size

      Traverser(Node<K,V>[] tab, int size, int index, int limit) {
         this.tab = tab;
         this.baseSize = size;
         this.baseIndex = this.index = index;
         this.baseLimit = limit;
         this.next = null;
      }

      /**
       * Advances if possible, returning next valid node, or null if none.
       */
      Node<K,V> advance() {
         Node<K,V> e;
         if ((e = next) != null)
            e = e.next;
         for (;;) {
            Node<K,V>[] t; int i, n;  // must use locals in checks
            if (e != null)
               return next = e;
            if (baseIndex >= baseLimit || (t = tab) == null ||
                  (n = t.length) <= (i = index) || i < 0)
               return next = null;
            if ((e = tabAt(t, i)) != null && e.hash < 0) {
               if (e instanceof ForwardingNode) {
                  tab = ((ForwardingNode<K,V>)e).nextTable;
                  e = null;
                  pushState(t, i, n);
                  continue;
               }
               else if (e instanceof TreeBin)
                  e = ((TreeBin<K,V>)e).first;
               else
                  e = null;
            }
            if (stack != null)
               recoverState(n);
            else if ((index = i + baseSize) >= n)
               index = ++baseIndex; // visit upper slots if present
         }
      }

      /**
       * Saves traversal state upon encountering a forwarding node.
       */
      private void pushState(Node<K,V>[] t, int i, int n) {
         TableStack<K,V> s = spare;  // reuse if possible
         if (s != null)
            spare = s.next;
         else
            s = new TableStack<K,V>();
         s.tab = t;
         s.length = n;
         s.index = i;
         s.next = stack;
         stack = s;
      }

      /**
       * Possibly pops traversal state.
       *
       * @param n length of current table
       */
      private void recoverState(int n) {
         TableStack<K,V> s; int len;
         while ((s = stack) != null && (index += (len = s.length)) >= n) {
            n = len;
            index = s.index;
            tab = s.tab;
            s.tab = null;
            TableStack<K,V> next = s.next;
            s.next = spare; // save for reuse
            stack = next;
            spare = s;
         }
         if (s == null && (index += baseSize) >= n)
            index = ++baseIndex;
      }
   }

   /**
    * Base of key, value, and entry Iterators. Adds fields to
    * Traverser to support iterator.remove.
    */
   static class BaseIterator<K,V> extends Traverser<K,V> {
      final BoundedEquivalentConcurrentHashMapV8<K,V> map;
      K lastKey;
      K key;
      V value;
      BaseIterator(Node<K,V>[] tab, int size, int index, int limit,
            BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         super(tab, size, index, limit);
         this.map = map;
         advanceUntilValidValue();
      }

      final Node<K, V> advance() {
         throw new UnsupportedOperationException();
      }

      final void advanceUntilValidValue() {
         Node<K, V> node = super.advance();
         if (node != null) {
            do {
               value = node.val;
               if (value != null && value != NULL_VALUE) {
                  key = node.key;
                  break;
               } else {
                  key = null;
               }
            } while ((node = super.advance()) != null);
         } else {
            key = null;
            value = null;
         }
      }

      public final boolean hasNext() { return key != null; }
      public final boolean hasMoreElements() { return key != null; }

      public final void remove() {
         K p;
         if ((p = lastKey) == null)
            throw new IllegalStateException();
         lastKey = null;
         map.replaceNode(p, null, null);
      }
   }

   static final class KeyIterator<K,V> extends BaseIterator<K,V>
         implements Iterator<K>, Enumeration<K> {
      KeyIterator(Node<K,V>[] tab, int index, int size, int limit,
            BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         super(tab, index, size, limit, map);
      }

      public final K next() {
         K k;
         if ((k = key) == null)
            throw new NoSuchElementException();
         lastKey = k;
         advanceUntilValidValue();
         return k;
      }

      public final K nextElement() { return next(); }
   }

   static final class ValueIterator<K,V> extends BaseIterator<K,V>
         implements Iterator<V>, Enumeration<V> {
      ValueIterator(Node<K,V>[] tab, int index, int size, int limit,
            BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         super(tab, index, size, limit, map);
      }

      public final V next() {
         K k;
         if ((k = key) == null)
            throw new NoSuchElementException();
         lastKey = k;
         V v = value;
         advanceUntilValidValue();
         return v;
      }

      public final V nextElement() { return next(); }
   }

   static final class EntryIterator<K,V> extends BaseIterator<K,V>
         implements Iterator<Map.Entry<K,V>> {
      EntryIterator(Node<K,V>[] tab, int index, int size, int limit,
            BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         super(tab, index, size, limit, map);
      }

      public final Map.Entry<K,V> next() {
         K k;
         if ((k = key) == null)
            throw new NoSuchElementException();
         V v = value;
         lastKey = k;
         advanceUntilValidValue();
         return new MapEntry<K,V>(k, v, map);
      }
   }

   /**
    * Exported Entry for EntryIterator
    */
   static final class MapEntry<K,V> implements Map.Entry<K,V> {
      final K key; // non-null
      V val;       // non-null
      final BoundedEquivalentConcurrentHashMapV8<K,V> map;
      MapEntry(K key, V val, BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         this.key = key;
         this.val = val;
         this.map = map;
      }
      public K getKey()        { return key; }
      public V getValue()      { return val; }
      public int hashCode()    { return map.keyEq.hashCode(key) ^ map.valueEq.hashCode(val); } // EQUIVALENCE_MOD
      public String toString() { return key + "=" + val; }

      public boolean equals(Object o) {
         Object k, v; Map.Entry<?,?> e;
         return ((o instanceof Map.Entry) &&
                       (k = (e = (Map.Entry<?,?>)o).getKey()) != null &&
                       (v = e.getValue()) != null &&
                       (k == key || map.keyEq.equals(key, k)) && // EQUIVALENCE_MOD
                       (v == val || map.valueEq.equals(val, v))); // EQUIVALENCE_MOD
      }

      /**
       * Sets our entry's value and writes through to the map. The
       * value to return is somewhat arbitrary here. Since we do not
       * necessarily track asynchronous changes, the most recent
       * "previous" value could be different from what we return (or
       * could even have been removed, in which case the put will
       * re-establish). We do not and cannot guarantee more.
       */
      public V setValue(V value) {
         if (value == null) throw new NullPointerException();
         V v = val;
         val = value;
         map.put(key, value);
         return v;
      }
   }

   static final class KeySpliterator<K,V> extends Traverser<K,V>
         implements ConcurrentHashMapSpliterator<K> {
      long est;               // size estimate
      KeySpliterator(Node<K,V>[] tab, int size, int index, int limit,
            long est) {
         super(tab, size, index, limit);
         this.est = est;
      }

      public ConcurrentHashMapSpliterator<K> trySplit() {
         int i, f, h;
         return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
               new KeySpliterator<K,V>(tab, baseSize, baseLimit = h,
                     f, est >>>= 1);
      }

      public void forEachRemaining(Consumer<? super K> action) {
         if (action == null) throw new NullPointerException();
         for (Node<K,V> p; (p = advance()) != null;)
            action.accept(p.key);
      }

      public boolean tryAdvance(Consumer<? super K> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V> p;
         if ((p = advance()) == null)
            return false;
         action.accept(p.key);
         return true;
      }

      public long estimateSize() { return est; }

   }

   static final class ValueSpliterator<K,V> extends Traverser<K,V>
         implements ConcurrentHashMapSpliterator<V> {
      long est;               // size estimate
      ValueSpliterator(Node<K,V>[] tab, int size, int index, int limit,
            long est) {
         super(tab, size, index, limit);
         this.est = est;
      }

      public ConcurrentHashMapSpliterator<V> trySplit() {
         int i, f, h;
         return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
               new ValueSpliterator<K,V>(tab, baseSize, baseLimit = h,
                     f, est >>>= 1);
      }

      public void forEachRemaining(Consumer<? super V> action) {
         if (action == null) throw new NullPointerException();
         for (Node<K,V> p; (p = advance()) != null;)
            action.accept(p.val);
      }

      public boolean tryAdvance(Consumer<? super V> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V> p;
         if ((p = advance()) == null)
            return false;
         action.accept(p.val);
         return true;
      }

      public long estimateSize() { return est; }

   }

   static final class EntrySpliterator<K,V> extends Traverser<K,V>
         implements ConcurrentHashMapSpliterator<Map.Entry<K,V>> {
      final BoundedEquivalentConcurrentHashMapV8<K,V> map; // To export MapEntry
      long est;               // size estimate
      EntrySpliterator(Node<K,V>[] tab, int size, int index, int limit,
            long est, BoundedEquivalentConcurrentHashMapV8<K,V> map) {
         super(tab, size, index, limit);
         this.map = map;
         this.est = est;
      }

      public ConcurrentHashMapSpliterator<Map.Entry<K,V>> trySplit() {
         int i, f, h;
         return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
               new EntrySpliterator<K,V>(tab, baseSize, baseLimit = h,
                     f, est >>>= 1, map);
      }

      public void forEachRemaining(Consumer<? super Map.Entry<K,V>> action) {
         if (action == null) throw new NullPointerException();
         for (Node<K,V> p; (p = advance()) != null; ) {
            V val = p.val;
            if (val != NULL_VALUE)
            action.accept(new MapEntry<K, V>(p.key, val, map));
         }
      }

      public boolean tryAdvance(Consumer<? super Map.Entry<K,V>> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V> p;
         while ((p = advance()) != null) {
            V val = p.val;
            if (val != NULL_VALUE) {
               action.accept(new MapEntry<K, V>(p.key, val, map));
               return true;
            }
         }
         return false;
      }

      public long estimateSize() { return est; }

   }

   // Parallel bulk operations

   /**
    * Computes initial batch value for bulk tasks. The returned value
    * is approximately exp2 of the number of times (minus one) to
    * split task by two before executing leaf action. This value is
    * faster to compute and more convenient to use as a guide to
    * splitting than is the depth, since it is used while dividing by
    * two anyway.
    */
   final int batchFor(long b) {
      long n;
      if (b == Long.MAX_VALUE || (n = sumCount()) <= 1L || n < b)
         return 0;
      int sp = ForkJoinPool.getCommonPoolParallelism() << 2; // slack of 4
      return (b <= 0L || (n /= b) >= sp) ? sp : (int)n;
   }

   /**
    * Performs the given action for each (key, value).
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param action the action
    * @since 1.8
    */
   public void forEach(long parallelismThreshold,
         BiConsumer<? super K,? super V> action) {
      if (action == null) throw new NullPointerException();
      new ForEachMappingTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  action).invoke();
   }

   /**
    * Performs the given action for each non-null transformation
    * of each (key, value).
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case the action is not applied)
    * @param action the action
    * @since 1.8
    */
   public <U> void forEach(long parallelismThreshold,
         BiFunction<? super K, ? super V, ? extends U> transformer,
         Consumer<? super U> action) {
      if (transformer == null || action == null)
         throw new NullPointerException();
      new ForEachTransformedMappingTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  transformer, action).invoke();
   }

   /**
    * Returns a non-null result from applying the given search
    * function on each (key, value), or null if none.  Upon
    * success, further element processing is suppressed and the
    * results of any other parallel invocations of the search
    * function are ignored.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param searchFunction a function returning a non-null
    * result on success, else null
    * @return a non-null result from applying the given search
    * function on each (key, value), or null if none
    * @since 1.8
    */
   public <U> U search(long parallelismThreshold,
         BiFunction<? super K, ? super V, ? extends U> searchFunction) {
      if (searchFunction == null) throw new NullPointerException();
      return new SearchMappingsTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  searchFunction, new AtomicReference<U>()).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all (key, value) pairs using the given reducer to
    * combine values, or null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case it is not combined)
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all (key, value) pairs
    * @since 1.8
    */
   public <U> U reduce(long parallelismThreshold,
         BiFunction<? super K, ? super V, ? extends U> transformer,
         BiFunction<? super U, ? super U, ? extends U> reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceMappingsTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all (key, value) pairs using the given reducer to
    * combine values, and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all (key, value) pairs
    * @since 1.8
    */
   public double reduceToDouble(long parallelismThreshold,
         ToDoubleBiFunction<? super K, ? super V> transformer,
         double basis,
         DoubleBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceMappingsToDoubleTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all (key, value) pairs using the given reducer to
    * combine values, and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all (key, value) pairs
    * @since 1.8
    */
   public long reduceToLong(long parallelismThreshold,
         ToLongBiFunction<? super K, ? super V> transformer,
         long basis,
         LongBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceMappingsToLongTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all (key, value) pairs using the given reducer to
    * combine values, and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all (key, value) pairs
    * @since 1.8
    */
   public int reduceToInt(long parallelismThreshold,
         ToIntBiFunction<? super K, ? super V> transformer,
         int basis,
         IntBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceMappingsToIntTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Performs the given action for each key.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param action the action
    * @since 1.8
    */
   public void forEachKey(long parallelismThreshold,
         Consumer<? super K> action) {
      if (action == null) throw new NullPointerException();
      new ForEachKeyTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  action).invoke();
   }

   /**
    * Performs the given action for each non-null transformation
    * of each key.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case the action is not applied)
    * @param action the action
    * @since 1.8
    */
   public <U> void forEachKey(long parallelismThreshold,
         Function<? super K, ? extends U> transformer,
         Consumer<? super U> action) {
      if (transformer == null || action == null)
         throw new NullPointerException();
      new ForEachTransformedKeyTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  transformer, action).invoke();
   }

   /**
    * Returns a non-null result from applying the given search
    * function on each key, or null if none. Upon success,
    * further element processing is suppressed and the results of
    * any other parallel invocations of the search function are
    * ignored.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param searchFunction a function returning a non-null
    * result on success, else null
    * @return a non-null result from applying the given search
    * function on each key, or null if none
    * @since 1.8
    */
   public <U> U searchKeys(long parallelismThreshold,
         Function<? super K, ? extends U> searchFunction) {
      if (searchFunction == null) throw new NullPointerException();
      return new SearchKeysTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  searchFunction, new AtomicReference<U>()).invoke();
   }

   /**
    * Returns the result of accumulating all keys using the given
    * reducer to combine values, or null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param reducer a commutative associative combining function
    * @return the result of accumulating all keys using the given
    * reducer to combine values, or null if none
    * @since 1.8
    */
   public K reduceKeys(long parallelismThreshold,
         BiFunction<? super K, ? super K, ? extends K> reducer) {
      if (reducer == null) throw new NullPointerException();
      return new ReduceKeysTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all keys using the given reducer to combine values, or
    * null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case it is not combined)
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all keys
    * @since 1.8
    */
   public <U> U reduceKeys(long parallelismThreshold,
         Function<? super K, ? extends U> transformer,
         BiFunction<? super U, ? super U, ? extends U> reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceKeysTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all keys using the given reducer to combine values, and
    * the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all keys
    * @since 1.8
    */
   public double reduceKeysToDouble(long parallelismThreshold,
         ToDoubleFunction<? super K> transformer,
         double basis,
         DoubleBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceKeysToDoubleTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all keys using the given reducer to combine values, and
    * the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all keys
    * @since 1.8
    */
   public long reduceKeysToLong(long parallelismThreshold,
         ToLongFunction<? super K> transformer,
         long basis,
         LongBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceKeysToLongTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all keys using the given reducer to combine values, and
    * the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all keys
    * @since 1.8
    */
   public int reduceKeysToInt(long parallelismThreshold,
         ToIntFunction<? super K> transformer,
         int basis,
         IntBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceKeysToIntTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Performs the given action for each value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param action the action
    * @since 1.8
    */
   public void forEachValue(long parallelismThreshold,
         Consumer<? super V> action) {
      if (action == null)
         throw new NullPointerException();
      new ForEachValueTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  action).invoke();
   }

   /**
    * Performs the given action for each non-null transformation
    * of each value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case the action is not applied)
    * @param action the action
    * @since 1.8
    */
   public <U> void forEachValue(long parallelismThreshold,
         Function<? super V, ? extends U> transformer,
         Consumer<? super U> action) {
      if (transformer == null || action == null)
         throw new NullPointerException();
      new ForEachTransformedValueTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  transformer, action).invoke();
   }

   /**
    * Returns a non-null result from applying the given search
    * function on each value, or null if none.  Upon success,
    * further element processing is suppressed and the results of
    * any other parallel invocations of the search function are
    * ignored.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param searchFunction a function returning a non-null
    * result on success, else null
    * @return a non-null result from applying the given search
    * function on each value, or null if none
    * @since 1.8
    */
   public <U> U searchValues(long parallelismThreshold,
         Function<? super V, ? extends U> searchFunction) {
      if (searchFunction == null) throw new NullPointerException();
      return new SearchValuesTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  searchFunction, new AtomicReference<U>()).invoke();
   }

   /**
    * Returns the result of accumulating all values using the
    * given reducer to combine values, or null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param reducer a commutative associative combining function
    * @return the result of accumulating all values
    * @since 1.8
    */
   public V reduceValues(long parallelismThreshold,
         BiFunction<? super V, ? super V, ? extends V> reducer) {
      if (reducer == null) throw new NullPointerException();
      return new ReduceValuesTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all values using the given reducer to combine values, or
    * null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case it is not combined)
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all values
    * @since 1.8
    */
   public <U> U reduceValues(long parallelismThreshold,
         Function<? super V, ? extends U> transformer,
         BiFunction<? super U, ? super U, ? extends U> reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceValuesTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all values using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all values
    * @since 1.8
    */
   public double reduceValuesToDouble(long parallelismThreshold,
         ToDoubleFunction<? super V> transformer,
         double basis,
         DoubleBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceValuesToDoubleTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all values using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all values
    * @since 1.8
    */
   public long reduceValuesToLong(long parallelismThreshold,
         ToLongFunction<? super V> transformer,
         long basis,
         LongBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceValuesToLongTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all values using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all values
    * @since 1.8
    */
   public int reduceValuesToInt(long parallelismThreshold,
         ToIntFunction<? super V> transformer,
         int basis,
         IntBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceValuesToIntTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Performs the given action for each entry.
    * <p>
    * NOTE: Due to the nature of some eviction algorithms this method must copy any
    * valid node before calling the provided Action.  Thus for performance it may be
    * advisable to use forEach instead.
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param action the action
    * @since 1.8
    */
   public void forEachEntry(long parallelismThreshold,
         Consumer<? super Map.Entry<K,V>> action) {
      if (action == null) throw new NullPointerException();
      new ForEachEntryTask<K,V>(null, batchFor(parallelismThreshold), 0, 0, table,
            action).invoke();
   }

   /**
    * Performs the given action for each non-null transformation
    * of each entry.
    *    * <p>
    * NOTE: Due to the nature of some eviction algorithms this method must copy any
    * valid node before calling the provided Action.  Thus for performance it may be
    * advisable to use forEach instead.
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case the action is not applied)
    * @param action the action
    * @since 1.8
    */
   public <U> void forEachEntry(long parallelismThreshold,
         Function<Map.Entry<K,V>, ? extends U> transformer,
         Consumer<? super U> action) {
      if (transformer == null || action == null)
         throw new NullPointerException();
      new ForEachTransformedEntryTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  transformer, action).invoke();
   }

   /**
    * Returns a non-null result from applying the given search
    * function on each entry, or null if none.  Upon success,
    * further element processing is suppressed and the results of
    * any other parallel invocations of the search function are
    * ignored.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param searchFunction a function returning a non-null
    * result on success, else null
    * @return a non-null result from applying the given search
    * function on each entry, or null if none
    * @since 1.8
    */
   public <U> U searchEntries(long parallelismThreshold,
         Function<Map.Entry<K,V>, ? extends U> searchFunction) {
      if (searchFunction == null) throw new NullPointerException();
      return new SearchEntriesTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  searchFunction, new AtomicReference<U>()).invoke();
   }

   /**
    * Returns the result of accumulating all entries using the
    * given reducer to combine values, or null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param reducer a commutative associative combining function
    * @return the result of accumulating all entries
    * @since 1.8
    */
   public Map.Entry<K,V> reduceEntries(long parallelismThreshold,
         BiFunction<Map.Entry<K,V>, Map.Entry<K,V>, ? extends Map.Entry<K,V>> reducer) {
      if (reducer == null) throw new NullPointerException();
      return new ReduceEntriesTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all entries using the given reducer to combine values,
    * or null if none.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element, or null if there is no transformation (in
    * which case it is not combined)
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all entries
    * @since 1.8
    */
   public <U> U reduceEntries(long parallelismThreshold,
         Function<Map.Entry<K,V>, ? extends U> transformer,
         BiFunction<? super U, ? super U, ? extends U> reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceEntriesTask<K,V,U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all entries using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all entries
    * @since 1.8
    */
   public double reduceEntriesToDouble(long parallelismThreshold,
         ToDoubleFunction<Entry<K,V>> transformer,
         double basis,
         DoubleBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceEntriesToDoubleTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all entries using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all entries
    * @since 1.8
    */
   public long reduceEntriesToLong(long parallelismThreshold,
         ToLongFunction<Entry<K,V>> transformer,
         long basis,
         LongBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceEntriesToLongTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }

   /**
    * Returns the result of accumulating the given transformation
    * of all entries using the given reducer to combine values,
    * and the given basis as an identity value.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param transformer a function returning the transformation
    * for an element
    * @param basis the identity (initial default value) for the reduction
    * @param reducer a commutative associative combining function
    * @return the result of accumulating the given transformation
    * of all entries
    * @since 1.8
    */
   public int reduceEntriesToInt(long parallelismThreshold,
         ToIntFunction<Entry<K,V>> transformer,
         int basis,
         IntBinaryOperator reducer) {
      if (transformer == null || reducer == null)
         throw new NullPointerException();
      return new MapReduceEntriesToIntTask<K,V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                  null, transformer, basis, reducer).invoke();
   }


    /* ----------------Views -------------- */

   /**
    * Base class for views.
    */
   abstract static class CollectionView<K,V,E>
         implements Collection<E>, java.io.Serializable {
      private static final long serialVersionUID = 7249069246763182397L;
      final BoundedEquivalentConcurrentHashMapV8<K,V> map;
      CollectionView(BoundedEquivalentConcurrentHashMapV8<K,V> map)  { this.map = map; }

      /**
       * Returns the map backing this view.
       *
       * @return the map backing this view
       */
      public BoundedEquivalentConcurrentHashMapV8<K,V> getMap() { return map; }

      /**
       * Removes all of the elements from this view, by removing all
       * the mappings from the map backing this view.
       */
      public final void clear()      { map.clear(); }
      public final int size()        { return map.size(); }
      public final boolean isEmpty() { return map.isEmpty(); }

      // implementations below rely on concrete classes supplying these
      // abstract methods
      /**
       * Returns a "weakly consistent" iterator that will never
       * throw {@link ConcurrentModificationException}, and
       * guarantees to traverse elements as they existed upon
       * construction of the iterator, and may (but is not
       * guaranteed to) reflect any modifications subsequent to
       * construction.
       */
      public abstract Iterator<E> iterator();
      public abstract boolean contains(Object o);
      public abstract boolean remove(Object o);

      private static final String oomeMsg = "Required array size too large";

      public final Object[] toArray() {
         long sz = map.mappingCount();
         if (sz > MAX_ARRAY_SIZE)
            throw new OutOfMemoryError(oomeMsg);
         int n = (int)sz;
         Object[] r = new Object[n];
         int i = 0;
         for (E e : this) {
            if (i == n) {
               if (n >= MAX_ARRAY_SIZE)
                  throw new OutOfMemoryError(oomeMsg);
               if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1)
                  n = MAX_ARRAY_SIZE;
               else
                  n += (n >>> 1) + 1;
               r = Arrays.copyOf(r, n);
            }
            r[i++] = e;
         }
         return (i == n) ? r : Arrays.copyOf(r, i);
      }

      @SuppressWarnings("unchecked")
      public final <T> T[] toArray(T[] a) {
         long sz = map.mappingCount();
         if (sz > MAX_ARRAY_SIZE)
            throw new OutOfMemoryError(oomeMsg);
         int m = (int)sz;
         T[] r = (a.length >= m) ? a :
               (T[])java.lang.reflect.Array
                     .newInstance(a.getClass().getComponentType(), m);
         int n = r.length;
         int i = 0;
         for (E e : this) {
            if (i == n) {
               if (n >= MAX_ARRAY_SIZE)
                  throw new OutOfMemoryError(oomeMsg);
               if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1)
                  n = MAX_ARRAY_SIZE;
               else
                  n += (n >>> 1) + 1;
               r = Arrays.copyOf(r, n);
            }
            r[i++] = (T)e;
         }
         if (a == r && i < n) {
            r[i] = null; // null-terminate
            return r;
         }
         return (i == n) ? r : Arrays.copyOf(r, i);
      }

      /**
       * Returns a string representation of this collection.
       * The string representation consists of the string representations
       * of the collection's elements in the order they are returned by
       * its iterator, enclosed in square brackets ({@code "[]"}).
       * Adjacent elements are separated by the characters {@code ", "}
       * (comma and space).  Elements are converted to strings as by
       * {@link String#valueOf(Object)}.
       *
       * @return a string representation of this collection
       */
      public final String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append('[');
         Iterator<E> it = iterator();
         if (it.hasNext()) {
            for (;;) {
               Object e = it.next();
               sb.append(e == this ? "(this Collection)" : e);
               if (!it.hasNext())
                  break;
               sb.append(',').append(' ');
            }
         }
         return sb.append(']').toString();
      }

      public final boolean containsAll(Collection<?> c) {
         if (c != this) {
            for (Object e : c) {
               if (e == null || !contains(e))
                  return false;
            }
         }
         return true;
      }

      public final boolean removeAll(Collection<?> c) {
         boolean modified = false;
         for (Iterator<E> it = iterator(); it.hasNext();) {
            if (c.contains(it.next())) {
               it.remove();
               modified = true;
            }
         }
         return modified;
      }

      public final boolean retainAll(Collection<?> c) {
         boolean modified = false;
         for (Iterator<E> it = iterator(); it.hasNext();) {
            if (!c.contains(it.next())) {
               it.remove();
               modified = true;
            }
         }
         return modified;
      }

   }

   /**
    * A view of a EquivalentConcurrentHashMapV8 as a {@link Set} of keys, in
    * which additions may optionally be enabled by mapping to a
    * common value.  This class cannot be directly instantiated.
    * See {@link #keySet() keySet()},
    * {@link #keySet(Object) keySet(V)},
    * {@link #newKeySet(int, Equivalence) newKeySet()},
    * {@link #newKeySet(int, int, Equivalence) newKeySet(int)}.
    *
    * @since 1.8
    */
   public static class KeySetView<K,V> extends CollectionView<K,V,K>
         implements Set<K>, java.io.Serializable {
      private static final long serialVersionUID = 7249069246763182397L;
      private final V value;
      KeySetView(BoundedEquivalentConcurrentHashMapV8<K,V> map, V value) {  // non-public
         super(map);
         this.value = value;
      }

      /**
       * Returns the default mapped value for additions,
       * or {@code null} if additions are not supported.
       *
       * @return the default mapped value for additions, or {@code null}
       * if not supported
       */
      public V getMappedValue() { return value; }

      /**
       * {@inheritDoc}
       * @throws NullPointerException if the specified key is null
       */
      public boolean contains(Object o) { return map.containsKey(o); }

      /**
       * Removes the key from this map view, by removing the key (and its
       * corresponding value) from the backing map.  This method does
       * nothing if the key is not in the map.
       *
       * @param  o the key to be removed from the backing map
       * @return {@code true} if the backing map contained the specified key
       * @throws NullPointerException if the specified key is null
       */
      public boolean remove(Object o) { return map.remove(o) != null; }

      /**
       * @return an iterator over the keys of the backing map
       */
      public Iterator<K> iterator() {
         Node<K,V>[] t;
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         int f = (t = m.table) == null ? 0 : t.length;
         return new KeyIterator<K,V>(t, f, 0, f, m);
      }

      /**
       * Adds the specified key to this set view by mapping the key to
       * the default mapped value in the backing map, if defined.
       *
       * @param e key to be added
       * @return {@code true} if this set changed as a result of the call
       * @throws NullPointerException if the specified key is null
       * @throws UnsupportedOperationException if no default mapped value
       * for additions was provided
       */
      public boolean add(K e) {
         V v;
         if ((v = value) == null)
            throw new UnsupportedOperationException();
         return map.putVal(e, v, true) == null;
      }

      /**
       * Adds all of the elements in the specified collection to this set,
       * as if by calling {@link #add} on each one.
       *
       * @param c the elements to be inserted into this set
       * @return {@code true} if this set changed as a result of the call
       * @throws NullPointerException if the collection or any of its
       * elements are {@code null}
       * @throws UnsupportedOperationException if no default mapped value
       * for additions was provided
       */
      public boolean addAll(Collection<? extends K> c) {
         boolean added = false;
         V v;
         if ((v = value) == null)
            throw new UnsupportedOperationException();
         for (K e : c) {
            if (map.putVal(e, v, true) == null)
               added = true;
         }
         return added;
      }

      public int hashCode() {
         int h = 0;
         for (K e : this)
            h += map.keyEq.hashCode(e);
         return h;
      }

      public boolean equals(Object o) {
         Set<?> c;
         return ((o instanceof Set) &&
                       ((c = (Set<?>)o) == this ||
                              (containsAll(c) && c.containsAll(this))));
      }

      public ConcurrentHashMapSpliterator<K> spliteratorV8() {
         Node<K,V>[] t;
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         long n = m.sumCount();
         int f = (t = m.table) == null ? 0 : t.length;
         // TODO: need to support NULL value
         return new KeySpliterator<K,V>(t, f, 0, f, n < 0L ? 0L : n);
      }

      public void forEach(Consumer<? super K> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V>[] t;
         if ((t = map.table) != null) {
            Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
            for (Node<K,V> p; (p = it.advance()) != null; )
               if (p.val != NULL_VALUE)
                  action.accept(p.key);
         }
      }
   }

   /**
    * A view of a EquivalentConcurrentHashMapV8 as a {@link Collection} of
    * values, in which additions are disabled. This class cannot be
    * directly instantiated. See {@link #values()}.
    */
   static final class ValuesView<K,V> extends CollectionView<K,V,V>
         implements Collection<V>, java.io.Serializable {
      private static final long serialVersionUID = 2249069246763182397L;
      ValuesView(BoundedEquivalentConcurrentHashMapV8<K,V> map) { super(map); }
      public final boolean contains(Object o) {
         return map.containsValue(o);
      }

      public final boolean remove(Object o) {
         if (o != null) {
            for (Iterator<V> it = iterator(); it.hasNext();) {
               if (map.valueEq.equals(it.next(), o)) { // EQUIVALENCE_MOD
                  it.remove();
                  return true;
               }
            }
         }
         return false;
      }

      public final Iterator<V> iterator() {
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         Node<K,V>[] t;
         int f = (t = m.table) == null ? 0 : t.length;
         return new ValueIterator<K,V>(t, f, 0, f, m);
      }

      public final boolean add(V e) {
         throw new UnsupportedOperationException();
      }
      public final boolean addAll(Collection<? extends V> c) {
         throw new UnsupportedOperationException();
      }

      public ConcurrentHashMapSpliterator<V> spliteratorV8() {
         Node<K,V>[] t;
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         long n = m.sumCount();
         int f = (t = m.table) == null ? 0 : t.length;
         return new ValueSpliterator<K,V>(t, f, 0, f, n < 0L ? 0L : n);
      }

      public void forEach(Consumer<? super V> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V>[] t;
         if ((t = map.table) != null) {
            Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
            for (Node<K,V> p; (p = it.advance()) != null; )
               action.accept(p.val);
         }
      }
   }

   /**
    * A view of a EquivalentConcurrentHashMapV8 as a {@link Set} of (key, value)
    * entries.  This class cannot be directly instantiated. See
    * {@link #entrySet()}.
    */
   static final class EntrySetView<K,V> extends CollectionView<K,V,Map.Entry<K,V>>
         implements Set<Map.Entry<K,V>>, java.io.Serializable {
      private static final long serialVersionUID = 2249069246763182397L;
      EntrySetView(BoundedEquivalentConcurrentHashMapV8<K,V> map) { super(map); }

      @SuppressWarnings("unchecked")
      public boolean contains(Object o) {
         Object k, v, r; Map.Entry<?,?> e;
         return ((o instanceof Map.Entry) &&
                       (k = (e = (Map.Entry<?,?>)o).getKey()) != null &&
                       (r = map.get(k)) != null &&
                       (v = e.getValue()) != null &&
                       (v == r || map.valueEq.equals((V) r, v))); // EQUIVALENCE_MOD
      }

      public boolean remove(Object o) {
         Object k, v; Map.Entry<?,?> e;
         return ((o instanceof Map.Entry) &&
                       (k = (e = (Map.Entry<?,?>)o).getKey()) != null &&
                       (v = e.getValue()) != null &&
                       map.remove(k, v));
      }

      /**
       * @return an iterator over the entries of the backing map
       */
      public Iterator<Map.Entry<K,V>> iterator() {
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         Node<K,V>[] t;
         int f = (t = m.table) == null ? 0 : t.length;
         return new EntryIterator<K,V>(t, f, 0, f, m);
      }

      public boolean add(Entry<K,V> e) {
         return map.putVal(e.getKey(), e.getValue(), false) == null;
      }

      public boolean addAll(Collection<? extends Entry<K,V>> c) {
         boolean added = false;
         for (Entry<K,V> e : c) {
            if (add(e))
               added = true;
         }
         return added;
      }

      public final int hashCode() {
         int h = 0;
         Node<K,V>[] t;
         if ((t = map.table) != null) {
            Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
            for (Node<K,V> p; (p = it.advance()) != null; ) {
               V val  = p.val;
               if (val != NULL_VALUE) {
                  h += p.hashCode(p.key, val);
               }
            }
         }
         return h;
      }

      public final boolean equals(Object o) {
         Set<?> c;
         return ((o instanceof Set) &&
                       ((c = (Set<?>)o) == this ||
                              (containsAll(c) && c.containsAll(this))));
      }

      public ConcurrentHashMapSpliterator<Map.Entry<K,V>> spliteratorV8() {
         Node<K,V>[] t;
         BoundedEquivalentConcurrentHashMapV8<K,V> m = map;
         long n = m.sumCount();
         int f = (t = m.table) == null ? 0 : t.length;
         // TODO: need to tweak spliterator for null values
         return new EntrySpliterator<K,V>(t, f, 0, f, n < 0L ? 0L : n, m);
      }

      public void forEach(Consumer<? super Map.Entry<K,V>> action) {
         if (action == null) throw new NullPointerException();
         Node<K,V>[] t;
         if ((t = map.table) != null) {
            Traverser<K,V> it = new Traverser<K,V>(t, t.length, 0, t.length);
            for (Node<K,V> p; (p = it.advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE) {
                  action.accept(new MapEntry<K,V>(p.key, val, map));
               }
            }
         }
      }

   }

   // -------------------------------------------------------

   /**
    * Base class for bulk tasks. Repeats some fields and code from
    * class Traverser, because we need to subclass CountedCompleter.
    */
   abstract static class BulkTask<K,V,R> extends CountedCompleter<R> {
      private static final long serialVersionUID = -3076449340738586169L;
      
      Node<K,V>[] tab;        // same as Traverser
      Node<K,V> next;
      int index;
      int baseIndex;
      int baseLimit;
      final int baseSize;
      int batch;              // split control

      BulkTask(BulkTask<K,V,?> par, int b, int i, int f, Node<K,V>[] t) {
         super(par);
         this.batch = b;
         this.index = this.baseIndex = i;
         if ((this.tab = t) == null)
            this.baseSize = this.baseLimit = 0;
         else if (par == null)
            this.baseSize = this.baseLimit = t.length;
         else {
            this.baseLimit = f;
            this.baseSize = par.baseSize;
         }
      }

      /**
       * Same as Traverser version
       */
      final Node<K,V> advance() {
         Node<K,V> e;
         if ((e = next) != null)
            e = e.next;
         for (;;) {
            Node<K,V>[] t; int i, n;  // must use locals in checks
            if (e != null)
               return next = e;
            if (baseIndex >= baseLimit || (t = tab) == null ||
                  (n = t.length) <= (i = index) || i < 0)
               return next = null;
            if ((e = tabAt(t, index)) != null && e.hash < 0) {
               if (e instanceof ForwardingNode) {
                  tab = ((ForwardingNode<K,V>)e).nextTable;
                  e = null;
                  continue;
               }
               else if (e instanceof TreeBin)
                  e = ((TreeBin<K,V>)e).first;
               else
                  e = null;
            }
            if ((index += baseSize) >= n)
               index = ++baseIndex;    // visit upper slots if present
         }
      }
   }

   /*
     * Task classes. Coded in a regular but ugly format/style to
     * simplify checks that each variant differs in the right way from
     * others. The null screenings exist because compilers cannot tell
     * that we've already null-checked task arguments, so we force
     * simplest hoisted bypass to help avoid convoluted traps.
     */
   @SuppressWarnings("serial")
   static final class ForEachKeyTask<K,V>
         extends BulkTask<K,V,Void> {
      final Consumer<? super K> action;
      ForEachKeyTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Consumer<? super K> action) {
         super(p, b, i, f, t);
         this.action = action;
      }
      public final void compute() {
         final Consumer<? super K> action;
         if ((action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachKeyTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null;)
               if (p.val != NULL_VALUE)
                  action.accept(p.key);
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachValueTask<K,V>
         extends BulkTask<K,V,Void> {
      final Consumer<? super V> action;
      ForEachValueTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Consumer<? super V> action) {
         super(p, b, i, f, t);
         this.action = action;
      }
      public final void compute() {
         final Consumer<? super V> action;
         if ((action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachValueTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null;) {
               V val = p.val;
               if (val != NULL_VALUE)
                  action.accept(val);
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachEntryTask<K,V>
         extends BulkTask<K,V,Void> {
      final Consumer<? super Entry<K,V>> action;
      ForEachEntryTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Consumer<? super Entry<K,V>> action) {
         super(p, b, i, f, t);
         this.action = action;
      }
      public final void compute() {
         final Consumer<? super Entry<K,V>> action;
         if ((action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachEntryTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE) {
                  // This is rather inefficient since we don't know if a node will
                  // be modified concurrently...
                  Node<K, V> copy = new Node<K, V>(p.hash, p.nodeEq, p.key, val, null);
                  action.accept(copy);
               }
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachMappingTask<K,V>
         extends BulkTask<K,V,Void> {
      final BiConsumer<? super K, ? super V> action;
      ForEachMappingTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  BiConsumer<? super K,? super V> action) {
         super(p, b, i, f, t);
         this.action = action;
      }
      public final void compute() {
         final BiConsumer<? super K, ? super V> action;
         if ((action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachMappingTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE)
                  action.accept(p.key, val);
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachTransformedKeyTask<K,V,U>
         extends BulkTask<K,V,Void> {
      final Function<? super K, ? extends U> transformer;
      final Consumer<? super U> action;
      ForEachTransformedKeyTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<? super K, ? extends U> transformer, Consumer<? super U> action) {
         super(p, b, i, f, t);
         this.transformer = transformer; this.action = action;
      }
      public final void compute() {
         final Function<? super K, ? extends U> transformer;
         final Consumer<? super U> action;
         if ((transformer = this.transformer) != null &&
               (action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachTransformedKeyTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           transformer, action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               if (p.val != NULL_VALUE) {
                  U u;
                  if ((u = transformer.apply(p.key)) != null)
                     action.accept(u);
               }
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachTransformedValueTask<K,V,U>
         extends BulkTask<K,V,Void> {
      final Function<? super V, ? extends U> transformer;
      final Consumer<? super U> action;
      ForEachTransformedValueTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<? super V, ? extends U> transformer, Consumer<? super U> action) {
         super(p, b, i, f, t);
         this.transformer = transformer; this.action = action;
      }
      public final void compute() {
         final Function<? super V, ? extends U> transformer;
         final Consumer<? super U> action;
         if ((transformer = this.transformer) != null &&
               (action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachTransformedValueTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           transformer, action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE) {
                  U u;
                  if ((u = transformer.apply(val)) != null)
                     action.accept(u);
               }
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachTransformedEntryTask<K,V,U>
         extends BulkTask<K,V,Void> {
      final Function<Map.Entry<K,V>, ? extends U> transformer;
      final Consumer<? super U> action;
      ForEachTransformedEntryTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<Map.Entry<K,V>, ? extends U> transformer, Consumer<? super U> action) {
         super(p, b, i, f, t);
         this.transformer = transformer; this.action = action;
      }
      public final void compute() {
         final Function<Map.Entry<K,V>, ? extends U> transformer;
         final Consumer<? super U> action;
         if ((transformer = this.transformer) != null &&
               (action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachTransformedEntryTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           transformer, action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE) {
                  // This is rather inefficient since we don't know if a node will
                  // be modified concurrently...
                  Node<K, V> copy = new Node<K, V>(p.hash, p.nodeEq, p.key, val, null);
                  U u;
                  if ((u = transformer.apply(copy)) != null)
                     action.accept(u);
               }
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ForEachTransformedMappingTask<K,V,U>
         extends BulkTask<K,V,Void> {
      final BiFunction<? super K, ? super V, ? extends U> transformer;
      final Consumer<? super U> action;
      ForEachTransformedMappingTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  BiFunction<? super K, ? super V, ? extends U> transformer,
                  Consumer<? super U> action) {
         super(p, b, i, f, t);
         this.transformer = transformer; this.action = action;
      }
      public final void compute() {
         final BiFunction<? super K, ? super V, ? extends U> transformer;
         final Consumer<? super U> action;
         if ((transformer = this.transformer) != null &&
               (action = this.action) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               new ForEachTransformedMappingTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           transformer, action).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != null && val != NULL_VALUE) {
                  U u;
                  if ((u = transformer.apply(p.key, val)) != null)
                     action.accept(u);
               }
            }
            propagateCompletion();
         }
      }
   }

   @SuppressWarnings("serial")
   static final class SearchKeysTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<? super K, ? extends U> searchFunction;
      final AtomicReference<U> result;
      SearchKeysTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<? super K, ? extends U> searchFunction,
                  AtomicReference<U> result) {
         super(p, b, i, f, t);
         this.searchFunction = searchFunction; this.result = result;
      }
      public final U getRawResult() { return result.get(); }
      public final void compute() {
         final Function<? super K, ? extends U> searchFunction;
         final AtomicReference<U> result;
         if ((searchFunction = this.searchFunction) != null &&
               (result = this.result) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               if (result.get() != null)
                  return;
               addToPendingCount(1);
               new SearchKeysTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           searchFunction, result).fork();
            }
            while (result.get() == null) {
               U u;
               Node<K,V> p;
               if ((p = advance()) == null) {
                  propagateCompletion();
                  break;
               }
               if ((u = searchFunction.apply(p.key)) != null) {
                  if (result.compareAndSet(null, u))
                     quietlyCompleteRoot();
                  break;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class SearchValuesTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<? super V, ? extends U> searchFunction;
      final AtomicReference<U> result;
      SearchValuesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<? super V, ? extends U> searchFunction,
                  AtomicReference<U> result) {
         super(p, b, i, f, t);
         this.searchFunction = searchFunction; this.result = result;
      }
      public final U getRawResult() { return result.get(); }
      public final void compute() {
         final Function<? super V, ? extends U> searchFunction;
         final AtomicReference<U> result;
         if ((searchFunction = this.searchFunction) != null &&
               (result = this.result) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               if (result.get() != null)
                  return;
               addToPendingCount(1);
               new SearchValuesTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           searchFunction, result).fork();
            }
            while (result.get() == null) {
               U u;
               Node<K,V> p;
               if ((p = advance()) == null) {
                  propagateCompletion();
                  break;
               }
               if ((u = searchFunction.apply(p.val)) != null) {
                  if (result.compareAndSet(null, u))
                     quietlyCompleteRoot();
                  break;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class SearchEntriesTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<Entry<K,V>, ? extends U> searchFunction;
      final AtomicReference<U> result;
      SearchEntriesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  Function<Entry<K,V>, ? extends U> searchFunction,
                  AtomicReference<U> result) {
         super(p, b, i, f, t);
         this.searchFunction = searchFunction; this.result = result;
      }
      public final U getRawResult() { return result.get(); }
      public final void compute() {
         final Function<Entry<K,V>, ? extends U> searchFunction;
         final AtomicReference<U> result;
         if ((searchFunction = this.searchFunction) != null &&
               (result = this.result) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               if (result.get() != null)
                  return;
               addToPendingCount(1);
               new SearchEntriesTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           searchFunction, result).fork();
            }
            while (result.get() == null) {
               U u;
               Node<K,V> p;
               if ((p = advance()) == null) {
                  propagateCompletion();
                  break;
               }
               if ((u = searchFunction.apply(p)) != null) {
                  if (result.compareAndSet(null, u))
                     quietlyCompleteRoot();
                  return;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class SearchMappingsTask<K,V,U>
         extends BulkTask<K,V,U> {
      final BiFunction<? super K, ? super V, ? extends U> searchFunction;
      final AtomicReference<U> result;
      SearchMappingsTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  BiFunction<? super K, ? super V, ? extends U> searchFunction,
                  AtomicReference<U> result) {
         super(p, b, i, f, t);
         this.searchFunction = searchFunction; this.result = result;
      }
      public final U getRawResult() { return result.get(); }
      public final void compute() {
         final BiFunction<? super K, ? super V, ? extends U> searchFunction;
         final AtomicReference<U> result;
         if ((searchFunction = this.searchFunction) != null &&
               (result = this.result) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               if (result.get() != null)
                  return;
               addToPendingCount(1);
               new SearchMappingsTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           searchFunction, result).fork();
            }
            while (result.get() == null) {
               U u;
               Node<K,V> p;
               if ((p = advance()) == null) {
                  propagateCompletion();
                  break;
               }
               if ((u = searchFunction.apply(p.key, p.val)) != null) {
                  if (result.compareAndSet(null, u))
                     quietlyCompleteRoot();
                  break;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ReduceKeysTask<K,V>
         extends BulkTask<K,V,K> {
      final BiFunction<? super K, ? super K, ? extends K> reducer;
      K result;
      ReduceKeysTask<K,V> rights, nextRight;
      ReduceKeysTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  ReduceKeysTask<K,V> nextRight,
                  BiFunction<? super K, ? super K, ? extends K> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.reducer = reducer;
      }
      public final K getRawResult() { return result; }
      public final void compute() {
         final BiFunction<? super K, ? super K, ? extends K> reducer;
         if ((reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new ReduceKeysTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, reducer)).fork();
            }
            K r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               K u = p.key;
               r = (r == null) ? u : u == null ? r : reducer.apply(r, u);
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") ReduceKeysTask<K,V>
                     t = (ReduceKeysTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  K tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ReduceValuesTask<K,V>
         extends BulkTask<K,V,V> {
      final BiFunction<? super V, ? super V, ? extends V> reducer;
      V result;
      ReduceValuesTask<K,V> rights, nextRight;
      ReduceValuesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  ReduceValuesTask<K,V> nextRight,
                  BiFunction<? super V, ? super V, ? extends V> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.reducer = reducer;
      }
      public final V getRawResult() { return result; }
      public final void compute() {
         final BiFunction<? super V, ? super V, ? extends V> reducer;
         if ((reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new ReduceValuesTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, reducer)).fork();
            }
            V r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               V v = p.val;
               r = (r == null) ? v : reducer.apply(r, v);
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") ReduceValuesTask<K,V>
                     t = (ReduceValuesTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  V tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class ReduceEntriesTask<K,V>
         extends BulkTask<K,V,Map.Entry<K,V>> {
      final BiFunction<Map.Entry<K,V>, Map.Entry<K,V>, ? extends Map.Entry<K,V>> reducer;
      Map.Entry<K,V> result;
      ReduceEntriesTask<K,V> rights, nextRight;
      ReduceEntriesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  ReduceEntriesTask<K,V> nextRight,
                  BiFunction<Entry<K,V>, Map.Entry<K,V>, ? extends Map.Entry<K,V>> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.reducer = reducer;
      }
      public final Map.Entry<K,V> getRawResult() { return result; }
      public final void compute() {
         final BiFunction<Map.Entry<K,V>, Map.Entry<K,V>, ? extends Map.Entry<K,V>> reducer;
         if ((reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new ReduceEntriesTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, reducer)).fork();
            }
            Map.Entry<K,V> r = null;
            for (Node<K,V> p; (p = advance()) != null; )
               r = (r == null) ? p : reducer.apply(r, p);
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") ReduceEntriesTask<K,V>
                     t = (ReduceEntriesTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  Map.Entry<K,V> tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceKeysTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<? super K, ? extends U> transformer;
      final BiFunction<? super U, ? super U, ? extends U> reducer;
      U result;
      MapReduceKeysTask<K,V,U> rights, nextRight;
      MapReduceKeysTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceKeysTask<K,V,U> nextRight,
                  Function<? super K, ? extends U> transformer,
                  BiFunction<? super U, ? super U, ? extends U> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.reducer = reducer;
      }
      public final U getRawResult() { return result; }
      public final void compute() {
         final Function<? super K, ? extends U> transformer;
         final BiFunction<? super U, ? super U, ? extends U> reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceKeysTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, reducer)).fork();
            }
            U r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               U u;
               if ((u = transformer.apply(p.key)) != null)
                  r = (r == null) ? u : reducer.apply(r, u);
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceKeysTask<K,V,U>
                     t = (MapReduceKeysTask<K,V,U>)c,
                     s = t.rights;
               while (s != null) {
                  U tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceValuesTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<? super V, ? extends U> transformer;
      final BiFunction<? super U, ? super U, ? extends U> reducer;
      U result;
      MapReduceValuesTask<K,V,U> rights, nextRight;
      MapReduceValuesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceValuesTask<K,V,U> nextRight,
                  Function<? super V, ? extends U> transformer,
                  BiFunction<? super U, ? super U, ? extends U> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.reducer = reducer;
      }
      public final U getRawResult() { return result; }
      public final void compute() {
         final Function<? super V, ? extends U> transformer;
         final BiFunction<? super U, ? super U, ? extends U> reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceValuesTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, reducer)).fork();
            }
            U r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               U u;
               if ((u = transformer.apply(p.val)) != null)
                  r = (r == null) ? u : reducer.apply(r, u);
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceValuesTask<K,V,U>
                     t = (MapReduceValuesTask<K,V,U>)c,
                     s = t.rights;
               while (s != null) {
                  U tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceEntriesTask<K,V,U>
         extends BulkTask<K,V,U> {
      final Function<Map.Entry<K,V>, ? extends U> transformer;
      final BiFunction<? super U, ? super U, ? extends U> reducer;
      U result;
      MapReduceEntriesTask<K,V,U> rights, nextRight;
      MapReduceEntriesTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceEntriesTask<K,V,U> nextRight,
                  Function<Map.Entry<K,V>, ? extends U> transformer,
                  BiFunction<? super U, ? super U, ? extends U> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.reducer = reducer;
      }
      public final U getRawResult() { return result; }
      public final void compute() {
         final Function<Map.Entry<K,V>, ? extends U> transformer;
         final BiFunction<? super U, ? super U, ? extends U> reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceEntriesTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, reducer)).fork();
            }
            U r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               U u;
               if ((u = transformer.apply(p)) != null)
                  r = (r == null) ? u : reducer.apply(r, u);
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceEntriesTask<K,V,U>
                     t = (MapReduceEntriesTask<K,V,U>)c,
                     s = t.rights;
               while (s != null) {
                  U tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceMappingsTask<K,V,U>
         extends BulkTask<K,V,U> {
      final BiFunction<? super K, ? super V, ? extends U> transformer;
      final BiFunction<? super U, ? super U, ? extends U> reducer;
      U result;
      MapReduceMappingsTask<K,V,U> rights, nextRight;
      MapReduceMappingsTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceMappingsTask<K,V,U> nextRight,
                  BiFunction<? super K, ? super V, ? extends U> transformer,
                  BiFunction<? super U, ? super U, ? extends U> reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.reducer = reducer;
      }
      public final U getRawResult() { return result; }
      public final void compute() {
         final BiFunction<? super K, ? super V, ? extends U> transformer;
         final BiFunction<? super U, ? super U, ? extends U> reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceMappingsTask<K,V,U>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, reducer)).fork();
            }
            U r = null;
            for (Node<K,V> p; (p = advance()) != null; ) {
               V val = p.val;
               if (val != NULL_VALUE) {
                  U u;
                  if ((u = transformer.apply(p.key, val)) != null)
                     r = (r == null) ? u : reducer.apply(r, u);
               }
            }
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceMappingsTask<K,V,U>
                     t = (MapReduceMappingsTask<K,V,U>)c,
                     s = t.rights;
               while (s != null) {
                  U tr, sr;
                  if ((sr = s.result) != null)
                     t.result = (((tr = t.result) == null) ? sr :
                                       reducer.apply(tr, sr));
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceKeysToDoubleTask<K,V>
         extends BulkTask<K,V,Double> {
      final ToDoubleFunction<? super K> transformer;
      final DoubleBinaryOperator reducer;
      final double basis;
      double result;
      MapReduceKeysToDoubleTask<K,V> rights, nextRight;
      MapReduceKeysToDoubleTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceKeysToDoubleTask<K,V> nextRight,
                  ToDoubleFunction<? super K> transformer,
                  double basis,
                  DoubleBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Double getRawResult() { return result; }
      public final void compute() {
         final ToDoubleFunction<? super K> transformer;
         final DoubleBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            double r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceKeysToDoubleTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.key));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceKeysToDoubleTask<K,V>
                     t = (MapReduceKeysToDoubleTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsDouble(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceValuesToDoubleTask<K,V>
         extends BulkTask<K,V,Double> {
      final ToDoubleFunction<? super V> transformer;
      final DoubleBinaryOperator reducer;
      final double basis;
      double result;
      MapReduceValuesToDoubleTask<K,V> rights, nextRight;
      MapReduceValuesToDoubleTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceValuesToDoubleTask<K,V> nextRight,
                  ToDoubleFunction<? super V> transformer,
                  double basis,
                  DoubleBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Double getRawResult() { return result; }
      public final void compute() {
         final ToDoubleFunction<? super V> transformer;
         final DoubleBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            double r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceValuesToDoubleTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceValuesToDoubleTask<K,V>
                     t = (MapReduceValuesToDoubleTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsDouble(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceEntriesToDoubleTask<K,V>
         extends BulkTask<K,V,Double> {
      final ToDoubleFunction<Map.Entry<K,V>> transformer;
      final DoubleBinaryOperator reducer;
      final double basis;
      double result;
      MapReduceEntriesToDoubleTask<K,V> rights, nextRight;
      MapReduceEntriesToDoubleTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceEntriesToDoubleTask<K,V> nextRight,
                  ToDoubleFunction<Map.Entry<K,V>> transformer,
                  double basis,
                  DoubleBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Double getRawResult() { return result; }
      public final void compute() {
         final ToDoubleFunction<Map.Entry<K,V>> transformer;
         final DoubleBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            double r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceEntriesToDoubleTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsDouble(r, transformer.applyAsDouble(p));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceEntriesToDoubleTask<K,V>
                     t = (MapReduceEntriesToDoubleTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsDouble(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceMappingsToDoubleTask<K,V>
         extends BulkTask<K,V,Double> {
      final ToDoubleBiFunction<? super K, ? super V> transformer;
      final DoubleBinaryOperator reducer;
      final double basis;
      double result;
      MapReduceMappingsToDoubleTask<K,V> rights, nextRight;
      MapReduceMappingsToDoubleTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceMappingsToDoubleTask<K,V> nextRight,
                  ToDoubleBiFunction<? super K, ? super V> transformer,
                  double basis,
                  DoubleBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Double getRawResult() { return result; }
      public final void compute() {
         final ToDoubleBiFunction<? super K, ? super V> transformer;
         final DoubleBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            double r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceMappingsToDoubleTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.key, p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceMappingsToDoubleTask<K,V>
                     t = (MapReduceMappingsToDoubleTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsDouble(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceKeysToLongTask<K,V>
         extends BulkTask<K,V,Long> {
      final ToLongFunction<? super K> transformer;
      final LongBinaryOperator reducer;
      final long basis;
      long result;
      MapReduceKeysToLongTask<K,V> rights, nextRight;
      MapReduceKeysToLongTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceKeysToLongTask<K,V> nextRight,
                  ToLongFunction<? super K> transformer,
                  long basis,
                  LongBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Long getRawResult() { return result; }
      public final void compute() {
         final ToLongFunction<? super K> transformer;
         final LongBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            long r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceKeysToLongTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsLong(r, transformer.applyAsLong(p.key));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceKeysToLongTask<K,V>
                     t = (MapReduceKeysToLongTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsLong(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceValuesToLongTask<K,V>
         extends BulkTask<K,V,Long> {
      final ToLongFunction<? super V> transformer;
      final LongBinaryOperator reducer;
      final long basis;
      long result;
      MapReduceValuesToLongTask<K,V> rights, nextRight;
      MapReduceValuesToLongTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceValuesToLongTask<K,V> nextRight,
                  ToLongFunction<? super V> transformer,
                  long basis,
                  LongBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Long getRawResult() { return result; }
      public final void compute() {
         final ToLongFunction<? super V> transformer;
         final LongBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            long r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceValuesToLongTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsLong(r, transformer.applyAsLong(p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceValuesToLongTask<K,V>
                     t = (MapReduceValuesToLongTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsLong(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceEntriesToLongTask<K,V>
         extends BulkTask<K,V,Long> {
      final ToLongFunction<Map.Entry<K,V>> transformer;
      final LongBinaryOperator reducer;
      final long basis;
      long result;
      MapReduceEntriesToLongTask<K,V> rights, nextRight;
      MapReduceEntriesToLongTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceEntriesToLongTask<K,V> nextRight,
                  ToLongFunction<Map.Entry<K,V>> transformer,
                  long basis,
                  LongBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Long getRawResult() { return result; }
      public final void compute() {
         final ToLongFunction<Map.Entry<K,V>> transformer;
         final LongBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            long r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceEntriesToLongTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsLong(r, transformer.applyAsLong(p));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceEntriesToLongTask<K,V>
                     t = (MapReduceEntriesToLongTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsLong(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceMappingsToLongTask<K,V>
         extends BulkTask<K,V,Long> {
      final ToLongBiFunction<? super K, ? super V> transformer;
      final LongBinaryOperator reducer;
      final long basis;
      long result;
      MapReduceMappingsToLongTask<K,V> rights, nextRight;
      MapReduceMappingsToLongTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceMappingsToLongTask<K,V> nextRight,
                  ToLongBiFunction<? super K, ? super V> transformer,
                  long basis,
                  LongBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Long getRawResult() { return result; }
      public final void compute() {
         final ToLongBiFunction<? super K, ? super V> transformer;
         final LongBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            long r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceMappingsToLongTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsLong(r, transformer.applyAsLong(p.key, p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceMappingsToLongTask<K,V>
                     t = (MapReduceMappingsToLongTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsLong(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceKeysToIntTask<K,V>
         extends BulkTask<K,V,Integer> {
      final ToIntFunction<? super K> transformer;
      final IntBinaryOperator reducer;
      final int basis;
      int result;
      MapReduceKeysToIntTask<K,V> rights, nextRight;
      MapReduceKeysToIntTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceKeysToIntTask<K,V> nextRight,
                  ToIntFunction<? super K> transformer,
                  int basis,
                  IntBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Integer getRawResult() { return result; }
      public final void compute() {
         final ToIntFunction<? super K> transformer;
         final IntBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            int r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceKeysToIntTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsInt(r, transformer.applyAsInt(p.key));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceKeysToIntTask<K,V>
                     t = (MapReduceKeysToIntTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsInt(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceValuesToIntTask<K,V>
         extends BulkTask<K,V,Integer> {
      final ToIntFunction<? super V> transformer;
      final IntBinaryOperator reducer;
      final int basis;
      int result;
      MapReduceValuesToIntTask<K,V> rights, nextRight;
      MapReduceValuesToIntTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceValuesToIntTask<K,V> nextRight,
                  ToIntFunction<? super V> transformer,
                  int basis,
                  IntBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Integer getRawResult() { return result; }
      public final void compute() {
         final ToIntFunction<? super V> transformer;
         final IntBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            int r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceValuesToIntTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsInt(r, transformer.applyAsInt(p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceValuesToIntTask<K,V>
                     t = (MapReduceValuesToIntTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsInt(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceEntriesToIntTask<K,V>
         extends BulkTask<K,V,Integer> {
      final ToIntFunction<Map.Entry<K,V>> transformer;
      final IntBinaryOperator reducer;
      final int basis;
      int result;
      MapReduceEntriesToIntTask<K,V> rights, nextRight;
      MapReduceEntriesToIntTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceEntriesToIntTask<K,V> nextRight,
                  ToIntFunction<Map.Entry<K,V>> transformer,
                  int basis,
                  IntBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Integer getRawResult() { return result; }
      public final void compute() {
         final ToIntFunction<Map.Entry<K,V>> transformer;
         final IntBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            int r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceEntriesToIntTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsInt(r, transformer.applyAsInt(p));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceEntriesToIntTask<K,V>
                     t = (MapReduceEntriesToIntTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsInt(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

   @SuppressWarnings("serial")
   static final class MapReduceMappingsToIntTask<K,V>
         extends BulkTask<K,V,Integer> {
      final ToIntBiFunction<? super K, ? super V> transformer;
      final IntBinaryOperator reducer;
      final int basis;
      int result;
      MapReduceMappingsToIntTask<K,V> rights, nextRight;
      MapReduceMappingsToIntTask
            (BulkTask<K,V,?> p, int b, int i, int f, Node<K,V>[] t,
                  MapReduceMappingsToIntTask<K,V> nextRight,
                  ToIntBiFunction<? super K, ? super V> transformer,
                  int basis,
                  IntBinaryOperator reducer) {
         super(p, b, i, f, t); this.nextRight = nextRight;
         this.transformer = transformer;
         this.basis = basis; this.reducer = reducer;
      }
      public final Integer getRawResult() { return result; }
      public final void compute() {
         final ToIntBiFunction<? super K, ? super V> transformer;
         final IntBinaryOperator reducer;
         if ((transformer = this.transformer) != null &&
               (reducer = this.reducer) != null) {
            int r = this.basis;
            for (int i = baseIndex, f, h; batch > 0 &&
                  (h = ((f = baseLimit) + i) >>> 1) > i;) {
               addToPendingCount(1);
               (rights = new MapReduceMappingsToIntTask<K,V>
                     (this, batch >>>= 1, baseLimit = h, f, tab,
                           rights, transformer, r, reducer)).fork();
            }
            for (Node<K,V> p; (p = advance()) != null; )
               r = reducer.applyAsInt(r, transformer.applyAsInt(p.key, p.val));
            result = r;
            CountedCompleter<?> c;
            for (c = firstComplete(); c != null; c = c.nextComplete()) {
               @SuppressWarnings("unchecked") MapReduceMappingsToIntTask<K,V>
                     t = (MapReduceMappingsToIntTask<K,V>)c,
                     s = t.rights;
               while (s != null) {
                  t.result = reducer.applyAsInt(t.result, s.result);
                  s = t.rights = s.nextRight;
               }
            }
         }
      }
   }

    /* ---------------- Counters -------------- */

   // Adapted from LongAdder and Striped64.
   // See their internal docs for explanation.

   // A padded cell for distributing counts
   static final class CounterCell {
      volatile long p0, p1, p2, p3, p4, p5, p6;
      volatile long value;
      volatile long q0, q1, q2, q3, q4, q5, q6;
      CounterCell(long x) { value = x; }
   }

   /**
    * Holder for the thread-local hash code determining which
    * CounterCell to use. The code is initialized via the
    * counterHashCodeGenerator, but may be moved upon collisions.
    */
   static final class CounterHashCode {
      int code;
   }

   /**
    * Generates initial value for per-thread CounterHashCodes.
    */
   static final AtomicInteger counterHashCodeGenerator = new AtomicInteger();

   /**
    * Increment for counterHashCodeGenerator. See class ThreadLocal
    * for explanation.
    */
   static final int SEED_INCREMENT = 0x61c88647;

   /**
    * Per-thread counter hash codes. Shared across all instances.
    */
   static final ThreadLocal<CounterHashCode> threadCounterHashCode =
         new ThreadLocal<CounterHashCode>();


   final long sumCount() {
      CounterCell[] as = counterCells; CounterCell a;
      long sum = baseCount;
      if (as != null) {
         for (int i = 0; i < as.length; ++i) {
            if ((a = as[i]) != null)
               sum += a.value;
         }
      }
      return sum;
   }

   // See LongAdder version for explanation
   private final void fullAddCount(long x, CounterHashCode hc,
         boolean wasUncontended) {
      int h;
      if (hc == null) {
         hc = new CounterHashCode();
         int s = counterHashCodeGenerator.addAndGet(SEED_INCREMENT);
         h = hc.code = (s == 0) ? 1 : s; // Avoid zero
         threadCounterHashCode.set(hc);
      }
      else
         h = hc.code;
      boolean collide = false;                // True if last slot nonempty
      for (;;) {
         CounterCell[] as; CounterCell a; int n; long v;
         if ((as = counterCells) != null && (n = as.length) > 0) {
            if ((a = as[(n - 1) & h]) == null) {
               if (cellsBusy == 0) {            // Try to attach new Cell
                  CounterCell r = new CounterCell(x); // Optimistic create
                  if (cellsBusy == 0 &&
                        U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                     boolean created = false;
                     try {               // Recheck under lock
                        CounterCell[] rs; int m, j;
                        if ((rs = counterCells) != null &&
                              (m = rs.length) > 0 &&
                              rs[j = (m - 1) & h] == null) {
                           rs[j] = r;
                           created = true;
                        }
                     } finally {
                        cellsBusy = 0;
                     }
                     if (created)
                        break;
                     continue;           // Slot is now non-empty
                  }
               }
               collide = false;
            }
            else if (!wasUncontended)       // CAS already known to fail
               wasUncontended = true;      // Continue after rehash
            else if (U.compareAndSwapLong(a, CELLVALUE, v = a.value, v + x))
               break;
            else if (counterCells != as || n >= NCPU)
               collide = false;            // At max size or stale
            else if (!collide)
               collide = true;
            else if (cellsBusy == 0 &&
                  U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
               try {
                  if (counterCells == as) {// Expand table unless stale
                     CounterCell[] rs = new CounterCell[n << 1];
                     for (int i = 0; i < n; ++i)
                        rs[i] = as[i];
                     counterCells = rs;
                  }
               } finally {
                  cellsBusy = 0;
               }
               collide = false;
               continue;                   // Retry with expanded table
            }
            h ^= h << 13;                   // Rehash
            h ^= h >>> 17;
            h ^= h << 5;
         }
         else if (cellsBusy == 0 && counterCells == as &&
               U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
            boolean init = false;
            try {                           // Initialize table
               if (counterCells == as) {
                  CounterCell[] rs = new CounterCell[2];
                  rs[h & 1] = new CounterCell(x);
                  counterCells = rs;
                  init = true;
               }
            } finally {
               cellsBusy = 0;
            }
            if (init)
               break;
         }
         else if (U.compareAndSwapLong(this, BASECOUNT, v = baseCount, v + x))
            break;                          // Fall back on using base
      }
      hc.code = h;                            // Record index for next time
   }

   // Unsafe mechanics
   private static final sun.misc.Unsafe U;
   private static final long SIZECTL;
   private static final long TRANSFERINDEX;
   private static final long BASECOUNT;
   private static final long CELLSBUSY;
   private static final long CELLVALUE;
   private static final long ABASE;
   private static final int ASHIFT;

   static {
      try {
         U = getUnsafe();
         Class<?> k = BoundedEquivalentConcurrentHashMapV8.class;
         SIZECTL = U.objectFieldOffset
               (k.getDeclaredField("sizeCtl"));
         TRANSFERINDEX = U.objectFieldOffset
               (k.getDeclaredField("transferIndex"));
         BASECOUNT = U.objectFieldOffset
               (k.getDeclaredField("baseCount"));
         CELLSBUSY = U.objectFieldOffset
               (k.getDeclaredField("cellsBusy"));
         Class<?> ck = CounterCell.class;
         CELLVALUE = U.objectFieldOffset
               (ck.getDeclaredField("value"));
         Class<?> ak = Node[].class;
         ABASE = U.arrayBaseOffset(ak);
         int scale = U.arrayIndexScale(ak);
         if ((scale & (scale - 1)) != 0)
            throw new Error("data type scale not a power of two");
         ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
      } catch (Exception e) {
         throw new Error(e);
      }
   }

   /**
    * Returns a sun.misc.Unsafe.  Suitable for use in a 3rd party package.
    * Replace with a simple call to Unsafe.getUnsafe when integrating
    * into a jdk.
    *
    * @return a sun.misc.Unsafe
    */
   static sun.misc.Unsafe getUnsafe() {
      try {
         return sun.misc.Unsafe.getUnsafe();
      } catch (SecurityException tryReflectionInstead) {}
      try {
         return java.security.AccessController.doPrivileged
               (new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                  public sun.misc.Unsafe run() throws Exception {
                     Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                     for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x))
                           return k.cast(x);
                     }
                     throw new NoSuchFieldError("the Unsafe");
                  }});
      } catch (java.security.PrivilegedActionException e) {
         throw new RuntimeException("Could not initialize intrinsics",
               e.getCause());
      }
   }
}
