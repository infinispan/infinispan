package org.horizon.container;

import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.InternalEntryFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;

/**
 * A data container that exposes an iterator that is ordered based on order of entry into the container, with the oldest
 * entries first.
 *
 * //TODO this is a temporary, crappy and *very* inefficient implementation.  Needs to be properly implemented
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class FIFODataContainer implements DataContainer {
   Map<Object, InternalCacheEntry> entries;

   public FIFODataContainer() {
      entries = Collections.synchronizedMap(new LinkedHashMap<Object, InternalCacheEntry>());
   }

   public InternalCacheEntry get(Object k) {
         InternalCacheEntry ice = entries.get(k);
         if (ice != null) {
            if (ice.isExpired()) {
               entries.remove(k);
               ice = null;
            } else ice.touch();
         }
         return ice;
   }

   public void put(Object k, Object v, long lifespan, long maxIdle) {
         InternalCacheEntry ice = entries.get(k);
         if (ice != null) {
            ice.setValue(v);
            ice = ice.setLifespan(lifespan).setMaxIdle(maxIdle);

            if (ice.isExpired()) {
               entries.remove(ice);
            } else {
               entries.put(k, ice);
            }
         } else {
            entries.put(k, InternalEntryFactory.create(k, v, lifespan, maxIdle));
         }
   }

   public boolean containsKey(Object k) {
         InternalCacheEntry ice = entries.get(k);
         if (ice != null && ice.isExpired()) {
            entries.remove(k);
            return false;
         } else {
            return ice != null;
         }
   }

   public InternalCacheEntry remove(Object k) {
         InternalCacheEntry ice = entries.remove(k);
         if (ice == null || ice.isExpired())
            return null;
         else
            return ice;
   }

   public int size() {
         return entries.size();
   }

   public void clear() {
         entries.clear();
   }

   public Set<Object> keySet() {
         return new HashSet<Object>(entries.keySet());
   }

   public void purgeExpired() {
      synchronized (entries) {
         for (Iterator<InternalCacheEntry> i = entries.values().iterator(); i.hasNext();) {
            InternalCacheEntry ice = i.next();
            if (ice.isExpired()) i.remove();
         }
      }
   }

   public Iterator<InternalCacheEntry> iterator() {
         return new LinkedList<InternalCacheEntry>(entries.values()).iterator();
   }
}
