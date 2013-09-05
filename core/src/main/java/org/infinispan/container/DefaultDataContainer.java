package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.metadata.Metadata;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer implements DataContainer {

   final protected ConcurrentMap<Object, InternalCacheEntry> entries;
   protected InternalEntryFactory entryFactory;
   final protected DefaultEvictionListener evictionListener;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private PersistenceManager pm;
   private TimeService timeService;

   public DefaultDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentMap(128, concurrencyLevel);
      evictionListener = null;
   }

   public DefaultDataContainer(int concurrencyLevel,
         Equivalence keyEq, Equivalence valueEq) {
      // If at least one comparing implementation give, use ComparingCHMv8
      entries = CollectionFactory.makeConcurrentMap(128, concurrencyLevel, keyEq, valueEq);
      evictionListener = null;
   }

   protected DefaultDataContainer(int concurrencyLevel, int maxEntries,
         EvictionStrategy strategy, EvictionThreadPolicy policy,
         Equivalence keyEquivalence, Equivalence valueEquivalence) {
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      Eviction eviction;
      switch (strategy) {
         case FIFO:
         case UNORDERED:
         case LRU:
            eviction = Eviction.LRU;
            break;
         case LIRS:
            eviction = Eviction.LIRS;
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }

      entries = new BoundedConcurrentHashMap<Object, InternalCacheEntry>(
            maxEntries, concurrencyLevel, eviction, evictionListener,
            keyEquivalence, valueEquivalence);
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
         InternalEntryFactory entryFactory, ActivationManager activator, PersistenceManager clm, TimeService timeService) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.pm = clm;
      this.timeService = timeService;
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
            EvictionStrategy strategy, EvictionThreadPolicy policy,
            Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, maxEntries, strategy,
            policy, keyEquivalence, valueEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel,
         Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, keyEquivalence, valueEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry peek(Object key) {
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry get(Object k) {
      InternalCacheEntry e = peek(k);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis)) {
            entries.remove(k);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public void put(Object k, Object v, Metadata metadata) {
      InternalCacheEntry e = entries.get(k);
      if (e != null) {
         e.setValue(v);
         InternalCacheEntry original = e;
         e = entryFactory.update(e, metadata);
         // we have the same instance. So we need to reincarnate.
         if (original == e) {
            e.reincarnate(timeService.wallClockTime());
         }
      } else {
         // this is a brand-new entry
         e = entryFactory.create(k, v, metadata);
      }
      entries.put(k, e);
   }

   @Override
   public boolean containsKey(Object k) {
      InternalCacheEntry ice = peek(k);
      if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
         entries.remove(k);
         ice = null;
      }
      return ice != null;
   }

   @Override
   public InternalCacheEntry remove(Object k) {
      InternalCacheEntry e = entries.remove(k);
      return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
   }

   @Override
   public int size() {
      return entries.size();
   }

   @Override
   public void clear() {
      entries.clear();
   }

   @Override
   public Set<Object> keySet() {
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Collection<Object> values() {
      return new Values();
   }

   @Override
   public Set<InternalCacheEntry> entrySet() {
      return new EntrySet();
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = timeService.wallClockTime();
      for (Iterator<InternalCacheEntry> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
         InternalCacheEntry e = purgeCandidates.next();
         if (e.isExpired(currentTimeMillis)) {
            purgeCandidates.remove();
         }
      }
   }

   @Override
   public Iterator<InternalCacheEntry> iterator() {
      return new EntryIterator(entries.values().iterator());
   }

   private final class DefaultEvictionListener implements EvictionListener<Object, InternalCacheEntry> {

      @Override
      public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {
         evictionManager.onEntryEviction(evicted);
      }

      @Override
      public void onEntryChosenForEviction(InternalCacheEntry entry) {
         passivator.passivate(entry);
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.activate(key);
      }

      @Override
      public void onEntryRemoved(Object key) {
         if (pm != null)
            pm.deleteFromAllStores(key, false);
      }
   }

   private static class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry> it){
         super(it);
      }

      @Override
      public InternalCacheEntry next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public static class EntryIterator implements Iterator<InternalCacheEntry> {

      private final Iterator<InternalCacheEntry> it;

      EntryIterator(Iterator<InternalCacheEntry> it){this.it=it;}

      @Override
      public InternalCacheEntry next() {
         return it.next();
      }

      @Override
      public boolean hasNext() {
         return it.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    *
    */
   private class EntrySet extends AbstractSet<InternalCacheEntry> {

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         @SuppressWarnings("rawtypes")
         Map.Entry e = (Map.Entry) o;
         InternalCacheEntry ice = entries.get(e.getKey());
         if (ice == null) {
            return false;
         }
         return ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }

      @Override
      public String toString() {
         return entries.toString();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    *
    */
   private class Values extends AbstractCollection<Object> {
      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }
   }

   private static class ValueIterator implements Iterator<Object> {
      Iterator<InternalCacheEntry> currentIterator;

      private ValueIterator(Iterator<InternalCacheEntry> it) {
         currentIterator = it;
      }

      @Override
      public boolean hasNext() {
         return currentIterator.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object next() {
         return currentIterator.next().getValue();
      }
   }

}
