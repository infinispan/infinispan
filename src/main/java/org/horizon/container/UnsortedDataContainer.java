/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.horizon.container;

import org.horizon.CacheException;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Stop;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The basic container.  Accepts null keys.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class UnsortedDataContainer implements DataContainer {
   // TODO this should ideally have its own hash table rather than delegate to a concurrent map, which in turn will wrap things up in Entries again.  Remove this unneccessary delegation
   // data with expiry and without expiry are stored in different maps, for efficiency.  E.g., so that when purging expired
   // stuff, we don't need to iterate through immortal data.
   final ConcurrentMap<Object, CachedValue> immortalData = new ConcurrentHashMap<Object, CachedValue>();
   final ConcurrentMap<Object, ExpirableCachedValue> expirableData = new ConcurrentHashMap<Object, ExpirableCachedValue>();
   private CacheLoaderManager clm;
   private CacheStore cacheStore;

   @Inject
   public void injectDependencies(CacheLoaderManager clm) {
      this.clm = clm;
   }

   private void expire(Object key) {
      expirableData.remove(key);
      expireOnCacheStore(key);
   }

   private void expireOnCacheStore(Object key) {
      if (cacheStore == null && clm != null) cacheStore = clm.getCacheStore();
      if (cacheStore != null) {
         try {
            cacheStore.remove(key);
         } catch (CacheLoaderException e) {
            throw new CacheException("Unable to expire entry in cache store", e);
         }
      }
   }

   public Object get(Object k) {
      CachedValue cv = immortalData.get(k);
      if (cv != null) {
         cv.touch();
         return cv.getValue();
      } else {
         ExpirableCachedValue ecv = expirableData.get(k);
         if (ecv != null) {
            if (ecv.isExpired()) {
               expire(k);
            } else {
               ecv.touch();
               return ecv.getValue();
            }
         }
      }

      return null;
   }

   public void put(Object k, Object v, long lifespan) {
      CachedValue cv = immortalData.get(k);
      ExpirableCachedValue ecv;
      if (cv != null) {
         // do we need to move this to expirable?
         if (lifespan < 0) {
            // no.
            cv.setValue(v);
            cv.touch();
         } else {
            ecv = new ExpirableCachedValue(v, lifespan);
            immortalData.remove(k);
            expirableData.put(k, ecv);
         }
      } else if ((ecv = expirableData.get(k)) != null) {
         // do we need to move this to immortal?
         if (lifespan < 0) {
            // yes.
            cv = new CachedValue(v);
            expirableData.remove(k);
            immortalData.put(k, cv);
         } else {
            ecv.setValue(v);
            ecv.touch();
         }
      } else {
         // does not exist anywhere!
         if (lifespan < 0) {
            cv = new CachedValue(v);
            immortalData.put(k, cv);
         } else {
            ecv = new ExpirableCachedValue(v, lifespan);
            expirableData.put(k, ecv);
         }
      }
   }

   public boolean containsKey(Object k) {
      if (!immortalData.containsKey(k)) {
         ExpirableCachedValue ecv = expirableData.get(k);
         if (ecv == null) return false;
         if (ecv.isExpired()) {
            expire(k);
            return false;
         }
      }
      return true;
   }

   public long getModifiedTimestamp(Object key) {
      CachedValue cv = immortalData.get(key);
      if (cv == null) cv = expirableData.get(key);
      return cv == null ? -1 : cv.getModifiedTime();
   }

   public Object remove(Object k) {
      CachedValue cv = immortalData.remove(k);
      if (cv == null) cv = expirableData.remove(k);

      if (cv == null) {
         return null;
      } else {
         return cv.getValue();
      }
   }

   public int size() {
      return immortalData.size() + expirableData.size();
   }

   @Stop(priority = 999)
   public void clear() {
      immortalData.clear();
      expirableData.clear();
   }

   public Set<Object> keySet() {
      return new KeySet();
   }

   public String toString() {
      return "Immortal Data: " + immortalData.toString() + "\n" + "Expirable Data: " + expirableData.toString();
   }

   public Set<Object> purgeExpiredEntries() {
      Set<Object> purged = new HashSet<Object>();
      for (Iterator<Map.Entry<Object, ExpirableCachedValue>> iter = expirableData.entrySet().iterator(); iter.hasNext();) {
         Map.Entry<Object, ExpirableCachedValue> entry = iter.next();
         ExpirableCachedValue cv = entry.getValue();
         if (cv.isExpired()) {
            expireOnCacheStore(entry.getKey());
            purged.add(entry.getKey());
            iter.remove();
         }
      }
      return purged;
   }

   public StoredEntry createEntryForStorage(Object key) {
      CachedValue immortal = immortalData.get(key);
      if (immortal != null)
         return new StoredEntry(key, immortal.getValue());
      ExpirableCachedValue ecv = expirableData.get(key);
      if (ecv == null) return null;
      if (ecv.isExpired()) {
         expirableData.remove(key);
         return null;
      }
      return new StoredEntry(key, ecv.getValue(), ecv.getCreatedTime(), ecv.getExpiryTime());
   }

   public CachedValue getEntry(Object key) {
      CachedValue immortal = immortalData.get(key);
      if (immortal != null)
         return immortal;
      ExpirableCachedValue ecv = expirableData.get(key);
      if (ecv == null) return null;
      if (ecv.isExpired()) {
         expirableData.remove(key);
         return null;
      }
      return ecv;
   }

   public Set<StoredEntry> getAllEntriesForStorage() {
      Set<StoredEntry> set = new HashSet<StoredEntry>(immortalData.size() + expirableData.size());
      for (Map.Entry<Object, CachedValue> entry : immortalData.entrySet())
         set.add(new StoredEntry(entry.getKey(), entry.getValue().getValue()));

      for (Iterator<Map.Entry<Object, ExpirableCachedValue>> it = expirableData.entrySet().iterator(); it.hasNext();) {
         Map.Entry<Object, ExpirableCachedValue> entry = it.next();
         ExpirableCachedValue ecv = entry.getValue();
         if (ecv.isExpired())
            it.remove();
         else
            set.add(new StoredEntry(entry.getKey(), ecv.getValue(), ecv.getCreatedTime(), ecv.getExpiryTime()));
      }
      return set;
   }

   private class KeySet extends AbstractSet<Object> {
      Set<Object> immortalKeys;
      Set<Object> expirableKeys;

      public KeySet() {
         immortalKeys = immortalData.keySet();
         expirableKeys = expirableData.keySet();
      }

      public Iterator<Object> iterator() {
         return new KeyIterator(immortalKeys.iterator(), expirableKeys.iterator());
      }

      public void clear() {
         throw new UnsupportedOperationException();
      }

      public boolean contains(Object o) {
         return immortalKeys.contains(o) || expirableKeys.contains(o);
      }

      public boolean remove(Object o) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return immortalKeys.size() + expirableKeys.size();
      }
   }

   private class KeyIterator implements Iterator<Object> {
      Iterator<Iterator<Object>> metaIterator;
      Iterator<Object> immortalIterator;
      Iterator<Object> expirableIterator;
      Iterator<Object> currentIterator;

      private KeyIterator(Iterator<Object> immortalIterator, Iterator<Object> expirableIterator) {
         List<Iterator<Object>> iterators = new ArrayList<Iterator<Object>>(2);
         iterators.add(immortalIterator);
         iterators.add(expirableIterator);
         metaIterator = iterators.iterator();

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
}