/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.container.entries;

import org.infinispan.container.versioning.EntryVersion;

/**
 * A factory for internal entries
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class InternalEntryFactory {

   public static InternalCacheEntry create(Object key, Object value) {
      return create(key, value, null);
   }

   public static InternalCacheEntry create(Object key, Object value, EntryVersion version) {
      return new ImmortalCacheEntry(key, value, version);
   }

   public static InternalCacheEntry create(Object key, Object value, long lifespan) {
      return create(key, value, null, lifespan);
   }

   public static InternalCacheEntry create(Object key, Object value, EntryVersion version, long lifespan) {
      return lifespan > -1 ? new MortalCacheEntry(key, value, version, lifespan) : new ImmortalCacheEntry(key, value, version);
   }

   public static InternalCacheEntry create(Object key, Object value, long lifespan, long maxIdle) {
      return create(key, value, null, lifespan, maxIdle);
   }

   public static InternalCacheEntry create(Object key, Object value, EntryVersion version, long lifespan, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value, version);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, version, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, version, maxIdle);
      return new TransientMortalCacheEntry(key, value, version, maxIdle, lifespan);
   }

   public static InternalCacheEntry create(Object key, Object value, long created, long lifespan, long lastUsed, long maxIdle) {
      return create(key, value, null, created, lifespan, lastUsed, maxIdle);
   }

   public static InternalCacheEntry create(Object key, Object value, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value, version);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, version, lifespan, created);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, version, maxIdle, lastUsed);
      return new TransientMortalCacheEntry(key, value, version, maxIdle, lifespan, lastUsed, created);
   }

   public static InternalCacheValue createValue(Object v) {
      return createValue(v, null);
   }

   public static InternalCacheValue createValue(Object v, EntryVersion version) {
      return new ImmortalCacheValue(v, version);
   }

   public static InternalCacheValue createValue(Object v, long created, long lifespan, long lastUsed, long maxIdle) {
      return createValue(v, null, created, lifespan, lastUsed, maxIdle);
   }

   public static InternalCacheValue createValue(Object v, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(v, version);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(v, version, created, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(v, version, maxIdle, lastUsed);
      return new TransientMortalCacheValue(v, version, created, lifespan, maxIdle, lastUsed);
   }

   public InternalCacheEntry createNewEntry(Object key, Object value, EntryVersion version, long lifespan, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0)
         return new ImmortalCacheEntry(key, value, version);

      if (lifespan > -1 && maxIdle < 0)
         return new MortalCacheEntry(key, value, version, lifespan);

      if (lifespan < 0 && maxIdle > -1)
         return new TransientCacheEntry(key, value, version, maxIdle);

      // else...
      return new TransientMortalCacheEntry(key, value, version, maxIdle, lifespan);
   }

   /**
    * Sets the values on the given internal cache entry, potentially reconstructing the entry to the most appropriate
    * type (Mortal, Immortal, Transient or TransientMortal) based on the lifespan and maxIdle being set.  As such,
    * callers must *always* assume that the InternalCacheEntry instance is being changed and must switch reference to
    * the instance being returned, even though this *may* not be a new instance at all.
    *
    * @param ice      cache entry to work on
    * @param lifespan lifespan to set
    * @param maxIdle  max idle to set
    * @return a cache entry
    */
   public InternalCacheEntry update(InternalCacheEntry ice, long lifespan, long maxIdle) {
      if (ice instanceof ImmortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return ice;
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof MortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion());
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               ice.setLifespan(lifespan);
               return ice;
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion());
            } else {
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion());
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), ice.getVersion(), lifespan);
            } else {
               ice.setLifespan(lifespan);
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         }
      }
      return ice;
   }
}
