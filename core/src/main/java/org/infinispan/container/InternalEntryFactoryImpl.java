/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedImmortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedImmortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedMortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedMortalCacheValue;
import org.infinispan.container.entries.versioned.VersionedTransientCacheEntry;
import org.infinispan.container.entries.versioned.VersionedTransientCacheValue;
import org.infinispan.container.entries.versioned.VersionedTransientMortalCacheEntry;
import org.infinispan.container.entries.versioned.VersionedTransientMortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

/**
 * An implementation that generates non-versioned entries
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class InternalEntryFactoryImpl implements InternalEntryFactory {

   @Override
   public InternalCacheEntry create(Object key, Object value, EntryVersion version) {
      if (version == null)
         return new ImmortalCacheEntry(key, value);
      else
         return new VersionedImmortalCacheEntry(key, value, version);
   }

   @Override
   public InternalCacheEntry create(CacheEntry cacheEntry) {
      return create(cacheEntry.getKey(), cacheEntry.getValue(),
            cacheEntry.getVersion(), cacheEntry.getLifespan(), cacheEntry.getMaxIdle());
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, InternalCacheEntry cacheEntry) {
      return create(key, value, cacheEntry.getVersion(), cacheEntry.getCreated(),
            cacheEntry.getLifespan(), cacheEntry.getLastUsed(), cacheEntry.getMaxIdle());
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle) {
      if (version == null) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
      } else {
         if (lifespan < 0 && maxIdle < 0) return new VersionedImmortalCacheEntry(key, value, version);
         if (lifespan > -1 && maxIdle < 0) return new VersionedMortalCacheEntry(key, value, version, lifespan, created);
         if (lifespan < 0 && maxIdle > -1) return new VersionedTransientCacheEntry(key, value, version, maxIdle, lastUsed);
         return new VersionedTransientMortalCacheEntry(key, value, version, maxIdle, lifespan, lastUsed, created);
      }
   }

   @Override
   public InternalCacheValue createValue(CacheEntry cacheEntry) {
      EntryVersion version = cacheEntry.getVersion();
      long lifespan = cacheEntry.getLifespan();
      long maxIdle = cacheEntry.getMaxIdle();
      if (version == null) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(cacheEntry.getValue());
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(cacheEntry.getValue(), -1, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(cacheEntry.getValue(), maxIdle, -1);
         return new TransientMortalCacheValue(cacheEntry.getValue(), -1, lifespan, maxIdle, -1);
      } else {
         if (lifespan < 0 && maxIdle < 0) return new VersionedImmortalCacheValue(cacheEntry.getValue(), cacheEntry.getVersion());
         if (lifespan > -1 && maxIdle < 0) return new VersionedMortalCacheValue(cacheEntry.getValue(), cacheEntry.getVersion(), -1, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new VersionedTransientCacheValue(cacheEntry.getValue(), cacheEntry.getVersion(), maxIdle, -1);
         return new VersionedTransientMortalCacheValue(cacheEntry.getValue(), cacheEntry.getVersion(), -1, lifespan, maxIdle, -1);
      }
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, EntryVersion version, long lifespan, long maxIdle) {
      if (version == null) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
      } else {
         if (lifespan < 0 && maxIdle < 0) return new VersionedImmortalCacheEntry(key, value, version);
         if (lifespan > -1 && maxIdle < 0) new VersionedMortalCacheEntry(key, value, version, lifespan);
         if (lifespan < 0 && maxIdle > -1) new VersionedTransientCacheEntry(key, value, version, maxIdle);
         return new VersionedTransientMortalCacheEntry(key, value, version, maxIdle, lifespan);
      }
   }


   @Override
   public InternalCacheEntry update(InternalCacheEntry ice, long lifespan, long maxIdle) {
      EntryVersion version = ice.getVersion();
      if (version == null)
         return updateUnversionedEntry(ice, lifespan, maxIdle);
      else
         return updateVersionedEntry(ice, lifespan, maxIdle, version);
   }

   private InternalCacheEntry updateUnversionedEntry(InternalCacheEntry ice, long lifespan, long maxIdle) {
      if (ice instanceof ImmortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return ice;
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof MortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               ice.setLifespan(lifespan);
               return ice;
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
            } else {
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
            } else {
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
            } else {
               ice.setLifespan(lifespan);
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         }
      }
      return ice;
   }

   private InternalCacheEntry updateVersionedEntry(InternalCacheEntry ice,
         long lifespan, long maxIdle, EntryVersion version) {
      if (ice instanceof ImmortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return ice;
            } else {
               return new VersionedTransientCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new VersionedMortalCacheEntry(ice.getKey(), ice.getValue(), version, lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new VersionedTransientMortalCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof MortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new VersionedImmortalCacheEntry(ice.getKey(), ice.getValue(), version);
            } else {
               return new VersionedTransientCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               ice.setLifespan(lifespan);
               return ice;
            } else {
               long ctm = System.currentTimeMillis();
               return new VersionedTransientMortalCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new VersionedImmortalCacheEntry(ice.getKey(), ice.getVersion(), version);
            } else {
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         } else {
            if (maxIdle < 0) {
               return new VersionedMortalCacheEntry(ice.getKey(), ice.getValue(), version, lifespan);
            } else {
               long ctm = System.currentTimeMillis();
               return new VersionedTransientMortalCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle, lifespan, ctm, ctm);
            }
         }
      } else if (ice instanceof TransientMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new VersionedImmortalCacheEntry(ice.getKey(), ice.getValue(), version);
            } else {
               return new VersionedTransientCacheEntry(ice.getKey(), ice.getValue(), version, maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new VersionedMortalCacheEntry(ice.getKey(), ice.getValue(), version, lifespan);
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
