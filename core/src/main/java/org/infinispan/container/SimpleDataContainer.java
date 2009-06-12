package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.Immutables;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple data container that does not order entries for eviction, implemented using two ConcurrentHashMaps, one for
 * mortal and one for immortal entries.
 * <p/>
 * This container does not support eviction, in that entries are unsorted.
 * <p/>
 * This implementation offers O(1) performance for all operations.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@ThreadSafe
public class SimpleDataContainer implements DataContainer {
   final ConcurrentMap<Object, InternalCacheEntry> immortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>();
   final ConcurrentMap<Object, InternalCacheEntry> mortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>();
   final AtomicInteger numEntries = new AtomicInteger(0);

   /**
    * Like a get, but doesn't check for expired entries
    *
    * @param key key to retrieve
    * @return an entry or null
    */
   private InternalCacheEntry peek(Object key) {
      InternalCacheEntry e = immortalEntries.get(key);
      if (e == null) e = mortalEntries.get(key);
      return e;
   }

   public InternalCacheEntry get(Object k) {
      InternalCacheEntry e = peek(k);
      if (e != null) {
         if (e.isExpired()) {
            mortalEntries.remove(k);
            numEntries.getAndDecrement();
            e = null;
         } else {
            e.touch();
         }
      }
      return e;
   }

   public void put(Object k, Object v, long lifespan, long maxIdle) {
      InternalCacheEntry e = immortalEntries.get(k);
      if (e != null) {
         e.setValue(v);
         if (lifespan > -1) e = e.setLifespan(lifespan);
         if (maxIdle > -1) e = e.setMaxIdle(maxIdle);

         if (e.canExpire()) {
            immortalEntries.remove(k);
            mortalEntries.put(k, e);
         }
      } else {
         e = mortalEntries.get(k);
         if (e != null) {
            e.setValue(v);
            InternalCacheEntry original = e;
            e = e.setLifespan(lifespan).setMaxIdle(maxIdle);

            if (!e.canExpire()) {
               mortalEntries.remove(k);
               immortalEntries.put(k, e);
            } else if (e != original) {
               // the entry has changed type, but still can expire!
               mortalEntries.put(k, e);
            }
         } else {
            // this is a brand-new entry
            numEntries.getAndIncrement();
            e = InternalEntryFactory.create(k, v, lifespan, maxIdle);
            if (e.canExpire())
               mortalEntries.put(k, e);
            else
               immortalEntries.put(k, e);

         }
      }
   }

   public boolean containsKey(Object k) {
      InternalCacheEntry ice = peek(k);
      if (ice != null && ice.isExpired()) {
         mortalEntries.remove(k);
         numEntries.getAndDecrement();
         ice = null;
      }
      return ice != null;
   }

   public InternalCacheEntry remove(Object k) {
      InternalCacheEntry e = immortalEntries.remove(k);
      if (e == null) e = mortalEntries.remove(k);
      if (e != null) numEntries.getAndDecrement();

      return e == null || e.isExpired() ? null : e;
   }

   public int size() {
      return numEntries.get();
   }

   @Stop(priority = 999)
   public void clear() {
      immortalEntries.clear();
      mortalEntries.clear();
      numEntries.set(0);
   }

   public Set<Object> keySet() {
      return new KeySet();
   }

   public Collection<Object> values() {
      return new Values();
   }

   public Set<Map.Entry> entrySet() {
      return new EntrySet();
   }

   public void purgeExpired() {
      for (Iterator<InternalCacheEntry> entries = mortalEntries.values().iterator(); entries.hasNext();) {
         InternalCacheEntry e = entries.next();
         if (e.isExpired()) {
            entries.remove();
            numEntries.getAndDecrement();
         }
      }
   }

   public Iterator<InternalCacheEntry> iterator() {
      return new EntryIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
   }

   private class KeySet extends AbstractSet<Object> {
      final Set<Object> immortalKeys;
      final Set<Object> mortalKeys;

      public KeySet() {
         immortalKeys = immortalEntries.keySet();
         mortalKeys = mortalEntries.keySet();
      }

      public Iterator<Object> iterator() {
         return new KeyIterator(immortalKeys.iterator(), mortalKeys.iterator());
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public boolean contains(Object o) {
         return immortalKeys.contains(o) || mortalKeys.contains(o);
      }

      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return immortalKeys.size() + mortalKeys.size();
      }
   }

   private class KeyIterator implements Iterator<Object> {
      Iterator<Iterator<Object>> metaIterator;
      Iterator<Object> currentIterator;

      private KeyIterator(Iterator<Object> immortalIterator, Iterator<Object> mortalIterator) {
         metaIterator = Arrays.asList(immortalIterator, mortalIterator).iterator();
         if (metaIterator.hasNext()) currentIterator = metaIterator.next();
      }

      public boolean hasNext() {
         boolean hasNext = currentIterator.hasNext();
         while (!hasNext && metaIterator.hasNext()) {
            currentIterator = metaIterator.next();
            hasNext = currentIterator.hasNext();
         }
         return hasNext;
      }

      @SuppressWarnings("unchecked")
      public Object next() {
         return currentIterator.next();
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private class EntrySet extends AbstractSet<Map.Entry> {
      public Iterator<Map.Entry> iterator() {
         return new ImmutableEntryIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
      }

      @Override
      public int size() {
         return immortalEntries.size() + mortalEntries.size();
      }
   }

   private class MortalInmortalIterator {
      Iterator<Iterator<InternalCacheEntry>> metaIterator;
      Iterator<InternalCacheEntry> currentIterator;
      InternalCacheEntry next;

      private MortalInmortalIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         metaIterator = Arrays.asList(immortalIterator, mortalIterator).iterator();
         if (metaIterator.hasNext()) currentIterator = metaIterator.next();
      }

      public boolean hasNext() {
         boolean hasNext = currentIterator.hasNext();
         while (!hasNext && metaIterator.hasNext()) {
            currentIterator = metaIterator.next();
            hasNext = currentIterator.hasNext();
         }
         return hasNext;
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private class EntryIterator extends MortalInmortalIterator implements Iterator<InternalCacheEntry> {
      private EntryIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      @SuppressWarnings("unchecked")
      public InternalCacheEntry next() {
         return currentIterator.next();
      }
   }

   private class ImmutableEntryIterator extends MortalInmortalIterator implements Iterator<Map.Entry> {
      private ImmutableEntryIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      public Map.Entry next() {
         return Immutables.immutableEntry(currentIterator.next());
      }
   }

   private class Values extends AbstractCollection<Object> {
      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(immortalEntries.values().iterator(), mortalEntries.values().iterator());
      }

      @Override
      public int size() {
         return immortalEntries.size() + mortalEntries.size();
      }
   }

   private class ValueIterator extends MortalInmortalIterator implements Iterator<Object> {
      private ValueIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
         super(immortalIterator, mortalIterator);
      }

      public Object next() {
         return currentIterator.next().getValue();
      }
   }
}
