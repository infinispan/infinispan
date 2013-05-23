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

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
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
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

import java.util.concurrent.TimeUnit;

/**
 * An implementation that generates non-versioned entries
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class InternalEntryFactoryImpl implements InternalEntryFactory {

   @Override
   public InternalCacheEntry create(Object key, Object value, Metadata metadata) {
      long lifespan = metadata.lifespan();
      long maxIdle = metadata.maxIdle();
      if (!isStoreMetadata(metadata)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
      } else {
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata);
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata);
         return new MetadataTransientMortalCacheEntry(key, value, metadata);
      }
   }

   @Override
   public InternalCacheEntry create(CacheEntry cacheEntry) {
      return create(cacheEntry.getKey(), cacheEntry.getValue(),
            cacheEntry.getMetadata(), cacheEntry.getLifespan(), cacheEntry.getMaxIdle());
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, InternalCacheEntry cacheEntry) {
      return create(key, value, cacheEntry.getMetadata(), cacheEntry.getCreated(),
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
         // If no metadata passed, assumed embedded metadata
         Metadata metadata = new EmbeddedMetadata.Builder()
               .lifespan(lifespan).maxIdle(maxIdle).version(version).build();
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, created);
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, lastUsed);
         return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
      }
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, Metadata metadata, long created, long lifespan, long lastUsed, long maxIdle) {
      if (!isStoreMetadata(metadata)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
      } else {
         // Metadata to store, take lifespan and maxIdle settings from it
         long metaLifespan = metadata.lifespan();
         long metaMaxIdle = metadata.maxIdle();
         if (metaLifespan < 0 && metaMaxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (metaLifespan > -1 && metaMaxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, created);
         if (metaLifespan < 0 && metaMaxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, lastUsed);
         return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
      }
   }

   @Override
   public InternalCacheValue createValue(CacheEntry cacheEntry) {
      Metadata metadata = cacheEntry.getMetadata();
      long lifespan = cacheEntry.getLifespan();
      long maxIdle = cacheEntry.getMaxIdle();
      if (!isStoreMetadata(metadata)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(cacheEntry.getValue());
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(cacheEntry.getValue(), -1, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(cacheEntry.getValue(), maxIdle, -1);
         return new TransientMortalCacheValue(cacheEntry.getValue(), -1, lifespan, maxIdle, -1);
      } else {
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheValue(cacheEntry.getValue(), cacheEntry.getMetadata());
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheValue(cacheEntry.getValue(), cacheEntry.getMetadata(), -1);
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheValue(cacheEntry.getValue(), cacheEntry.getMetadata(), -1);
         return new MetadataTransientMortalCacheValue(cacheEntry.getValue(), cacheEntry.getMetadata(), -1, -1);
      }
   }

   @Override
   // TODO: Do we need this???
   public InternalCacheEntry create(Object key, Object value, Metadata metadata, long lifespan, long maxIdle) {
      if (!isStoreMetadata(metadata)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
      } else {
         // Metadata to store, take lifespan and maxIdle settings from it
         long metaLifespan = metadata.lifespan();
         long metaMaxIdle = metadata.maxIdle();
         if (metaLifespan < 0 && metaMaxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (metaLifespan > -1 && metaMaxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata);
         if (metaLifespan < 0 && metaMaxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata);
         return new MetadataTransientMortalCacheEntry(key, value, metadata);
      }
   }


   @Override
   public InternalCacheEntry update(InternalCacheEntry ice, Metadata metadata) {
      if (!isStoreMetadata(metadata))
         return updateMetadataUnawareEntry(ice, metadata.lifespan(), metadata.maxIdle());
      else
         return updateMetadataAwareEntry(ice, metadata);
   }

   private InternalCacheEntry updateMetadataUnawareEntry(InternalCacheEntry ice, long lifespan, long maxIdle) {
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
               ((MortalCacheEntry) ice).setLifespan(lifespan);
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
               ((TransientCacheEntry) ice).setMaxIdle(maxIdle);
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
               TransientMortalCacheEntry transientMortalEntry = (TransientMortalCacheEntry) ice;
               transientMortalEntry.setLifespan(lifespan);
               transientMortalEntry.setMaxIdle(maxIdle);
               return ice;
            }
         }
      }
      return ice;
   }

   private InternalCacheEntry updateMetadataAwareEntry(InternalCacheEntry ice, Metadata metadata) {
      long lifespan = metadata.lifespan();
      long maxIdle = metadata.maxIdle();
      if (ice instanceof MetadataImmortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               ice.setMetadata(metadata);
               return ice;
            } else {
               return new MetadataTransientCacheEntry(ice.getKey(), ice.getValue(), metadata);
            }
         } else {
            if (maxIdle < 0) {
               return new MetadataMortalCacheEntry(ice.getKey(), ice.getValue(), metadata);
            } else {
               long ctm = System.currentTimeMillis();
               return new MetadataTransientMortalCacheEntry(ice.getKey(), ice.getValue(), metadata, ctm, ctm);
            }
         }
      } else if (ice instanceof MetadataMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new MetadataImmortalCacheEntry(ice.getKey(), ice.getValue(), metadata);
            } else {
               return new MetadataTransientCacheEntry(ice.getKey(), ice.getValue(), metadata);
            }
         } else {
            if (maxIdle < 0) {
               ice.setMetadata(metadata);
               return ice;
            } else {
               long ctm = System.currentTimeMillis();
               return new MetadataTransientMortalCacheEntry(ice.getKey(), ice.getValue(), metadata, ctm, ctm);
            }
         }
      } else if (ice instanceof MetadataTransientCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new MetadataImmortalCacheEntry(ice.getKey(), ice.getValue(), metadata);
            } else {
               ice.setMetadata(metadata);
               return ice;
            }
         } else {
            if (maxIdle < 0) {
               return new MetadataMortalCacheEntry(ice.getKey(), ice.getValue(), metadata);
            } else {
               long ctm = System.currentTimeMillis();
               return new MetadataTransientMortalCacheEntry(ice.getKey(), ice.getValue(), metadata, ctm, ctm);
            }
         }
      } else if (ice instanceof MetadataTransientMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               return new MetadataImmortalCacheEntry(ice.getKey(), ice.getValue(), metadata);
            } else {
               return new MetadataTransientCacheEntry(ice.getKey(), ice.getValue(), metadata, maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MetadataMortalCacheEntry(ice.getKey(), ice.getValue(), metadata, lifespan);
            } else {
               ice.setMetadata(metadata);
               return ice;
            }
         }
      }
      return ice;
   }

   /**
    * Indicates whether the entire metadata object needs to be stored or not.
    *
    * This check is done to avoid keeping the entire metadata object around
    * when only lifespan or maxIdle time is stored. If more information
    * needs to be stored (i.e. version), or the metadata object is not the
    * embedded one, keep the entire metadata object around.
    *
    * @return true if the entire metadata object needs to be stored, otherwise
    * simply store lifespan and/or maxIdle in existing cache entries
    */
   private boolean isStoreMetadata(Metadata metadata) {
      return metadata != null
            && (metadata.version() != null
                      || !(metadata instanceof EmbeddedMetadata));
   }

}
