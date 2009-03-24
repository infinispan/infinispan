package org.horizon.container;

import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.InternalEntryFactory;
import org.horizon.factories.annotations.Stop;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple data container that does not order entries for eviction
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SimpleDataContainer implements DataContainer {
   final ConcurrentMap<Object, InternalCacheEntry> immortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>();
   final ConcurrentMap<Object, InternalCacheEntry> mortalEntries = new ConcurrentHashMap<Object, InternalCacheEntry>();

   public InternalCacheEntry get(Object k) {
      InternalCacheEntry e = immortalEntries.get(k);
      if (e == null) {
         e = mortalEntries.get(k);
         if (e != null) {
            if (e.isExpired()) {
               mortalEntries.remove(k);
               e = null;
            } else {
               e.touch();
            }
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
            e = e.setLifespan(lifespan).setMaxIdle(maxIdle);

            if (!e.canExpire()) {
               mortalEntries.remove(k);
               immortalEntries.put(k, e);
            }
         } else {
            e = InternalEntryFactory.create(k, v, lifespan, maxIdle);
            if (e.canExpire())
               mortalEntries.put(k, e);
            else
               immortalEntries.put(k, e);

         }
      }
   }

   public boolean containsKey(Object k) {
      return get(k) != null;
   }

   public InternalCacheEntry remove(Object k) {
      InternalCacheEntry e = immortalEntries.remove(k);
      if (e == null) {
         e = mortalEntries.remove(k);
      }

      return e == null || e.isExpired() ? null : e;
   }

   public int size() {
      return immortalEntries.size() + mortalEntries.size();
   }

   @Stop(priority = 999)
   public void clear() {
      immortalEntries.clear();
      mortalEntries.clear();
   }

   public Set<Object> keySet() {
      return new KeySet();
   }

   public void purge() {
      for (Iterator<InternalCacheEntry> entries = mortalEntries.values().iterator(); entries.hasNext();) {
         InternalCacheEntry e = entries.next();
         if (e.isExpired()) entries.remove();
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

   private class EntryIterator implements Iterator<InternalCacheEntry> {
      Iterator<Iterator<InternalCacheEntry>> metaIterator;
      Iterator<InternalCacheEntry> currentIterator;
      InternalCacheEntry next;

      private EntryIterator(Iterator<InternalCacheEntry> immortalIterator, Iterator<InternalCacheEntry> mortalIterator) {
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
      public InternalCacheEntry next() {
         return currentIterator.next();
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }
}
