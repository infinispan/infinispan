package org.infinispan.container.entries;

/**
 * A factory for internal entries
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class InternalEntryFactory {

   boolean recordCreation, recordLastUsed;

   public InternalEntryFactory(boolean recordCreation, boolean recordLastUsed) {
      this.recordCreation = recordCreation;
      this.recordLastUsed = recordLastUsed;
   }

   public static final InternalCacheEntry create(Object key, Object value) {
      return new ImmortalCacheEntry(key, value);
   }

   public static final InternalCacheEntry create(Object key, Object value, long lifespan) {
      return lifespan > -1 ? new MortalCacheEntry(key, value, lifespan) : new ImmortalCacheEntry(key, value);
   }

   public static final InternalCacheEntry create(Object key, Object value, long lifespan, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle);
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
   }

   public static final InternalCacheEntry create(Object key, Object value, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
   }

   public static final InternalCacheValue createValue(Object v) {
      return new ImmortalCacheValue(v);
   }

   public static final InternalCacheValue createValue(Object v, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(v);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(v, created, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(v, maxIdle, lastUsed);
      return new TransientMortalCacheValue(v, created, lifespan, maxIdle, lastUsed);
   }

   public InternalCacheEntry createNewEntry(Object key, Object value, long lifespan, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) {
         if (recordCreation || recordLastUsed) {
            if (recordCreation && !recordLastUsed) return new MortalCacheEntry(key, value, -1);
            if (!recordCreation && recordLastUsed) return new TransientCacheEntry(key, value, -1);
            return new TransientMortalCacheEntry(key, value, -1, -1);
         } else {
            return new ImmortalCacheEntry(key, value);
         }
      }

      if (lifespan > -1 && maxIdle < 0) {
         if (recordLastUsed) {
            return new TransientMortalCacheEntry(key, value, -1, lifespan);
         } else {
            return new MortalCacheEntry(key, value, lifespan);
         }
      }

      if (lifespan < 0 && maxIdle > -1) {
         if (recordCreation) {
            return new TransientMortalCacheEntry(key, value, maxIdle, -1);
         } else {
            return new TransientCacheEntry(key, value, maxIdle);
         }
      }

      // else...
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
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
               return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
            }
         } else {
            if (maxIdle < 0) {
               return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
            } else {
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, System.currentTimeMillis(), ice.getCreated());
            }
         }
      } else if (ice instanceof MortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               if (recordCreation) {
                  ice.setLifespan(-1);
                  return ice;
               } else {
                  return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
               }
            } else {
               if (recordCreation) {
                  return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, -1, System.currentTimeMillis(), ice.getCreated());
               } else {
                  return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
               }
            }
         } else {
            if (maxIdle < 0) {
               ice.setLifespan(lifespan);
               return ice;
            } else {
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, System.currentTimeMillis(), ice.getCreated());
            }
         }
      } else if (ice instanceof TransientCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               if (recordLastUsed) {
                  ice.setMaxIdle(-1);
                  return ice;
               } else {
                  return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
               }
            } else {
               ice.setMaxIdle(maxIdle);
               return ice;
            }
         } else {
            if (maxIdle < 0) {
               if (recordLastUsed) {
                  return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, ice.getLastUsed(), System.currentTimeMillis());
               } else {
                  return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
               }
            } else {
               return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, lifespan, System.currentTimeMillis(), ice.getCreated());
            }
         }
      } else if (ice instanceof TransientMortalCacheEntry) {
         if (lifespan < 0) {
            if (maxIdle < 0) {
               if (recordCreation || recordLastUsed) {
                  if (recordLastUsed && recordCreation) {
                     ice.setLifespan(lifespan);
                     ice.setMaxIdle(maxIdle);
                     return ice;
                  } else if (recordCreation) {
                     return new MortalCacheEntry(ice.getKey(), ice.getValue(), -1, ice.getCreated());
                  } else {
                     return new TransientCacheEntry(ice.getKey(), ice.getValue(), -1, ice.getLastUsed());
                  }
               } else {
                  return new ImmortalCacheEntry(ice.getKey(), ice.getValue());
               }
            } else {
               if (recordCreation) {
                  return new TransientMortalCacheEntry(ice.getKey(), ice.getValue(), maxIdle, -1, System.currentTimeMillis(), ice.getCreated());
               } else {
                  return new TransientCacheEntry(ice.getKey(), ice.getValue(), maxIdle);
               }
            }
         } else {
            if (maxIdle < 0) {
               if (recordLastUsed) {
                  ice.setLifespan(lifespan);
                  ice.setMaxIdle(maxIdle);
                  return ice;
               } else {
                  return new MortalCacheEntry(ice.getKey(), ice.getValue(), lifespan);
               }
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
