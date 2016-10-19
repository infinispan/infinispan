package org.infinispan.marshall.core;

import java.util.Arrays;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An efficient identity object map whose keys are {@link Class} objects and
 * whose values are {@link AdvancedExternalizer} instances.
 */
final class ClassToExternalizerMap {

   private static final Log log = LogFactory.getLog(ClassToExternalizerMap.class);

   private AdvancedExternalizer[] values;
   private Class[] keys;
   private int count;
   private int resizeCount;
   private final float loadFactor;

   /**
    * Construct a new instance with the given initial capacity and load factor.
    *
    * @param initialCapacity the initial capacity
    * @param loadF the load factor
    */
   public ClassToExternalizerMap(int initialCapacity, final float loadF) {
      if (initialCapacity < 1) {
         throw new IllegalArgumentException("initialCapacity must be > 0");
      }
      if (loadF <= 0.0f || loadF >= 1.0f) {
         throw new IllegalArgumentException("loadFactor must be > 0.0 and < 1.0");
      }
      if (initialCapacity < 16) {
         initialCapacity = 16;
      } else {
         // round up
         final int c = Integer.highestOneBit(initialCapacity) - 1;
         initialCapacity = Integer.highestOneBit(initialCapacity + c);
      }
      keys = new Class[initialCapacity];
      values = new AdvancedExternalizer[initialCapacity];
      resizeCount = (int) ((double) initialCapacity * (double) loadF);
      this.loadFactor = loadF;
   }

   /**
    * Construct a new instance with the given load factor and an initial capacity of 64.
    *
    * @param loadFactor the load factor
    */
   public ClassToExternalizerMap(final float loadFactor) {
      this(64, loadFactor);
   }

   /**
    * Construct a new instance with the given initial capacity and a load factor of {@code 0.5}.
    *
    * @param initialCapacity the initial capacity
    */
   public ClassToExternalizerMap(final int initialCapacity) {
      this(initialCapacity, 0.5f);
   }

   /**
    * Construct a new instance with an initial capacity of 64 and a load factor of {@code 0.5}.
    */
   public ClassToExternalizerMap() {
      this(0.5f);
   }

   /**
    * Get a value from the map.
    *
    * @param key the key
    * @return the map value at the given key, or null if it's not found
    */
   public AdvancedExternalizer get(Class key) {
      final Class[] keys = this.keys;
      final int mask = keys.length - 1;
      int hc = System.identityHashCode(key) & mask;
      Class k;
      for (;;) {
         k = keys[hc];
         if (k == key) {
            return values[hc];
         }
         if (k == null) {
            // not found
            return null;
         }
         hc = (hc + 1) & mask;
      }
   }

   /**
    * Put a value into the map.  Any previous mapping is discarded silently.
    *
    * @param key the key
    * @param value the value to store
    */
   public void put(Class key, AdvancedExternalizer value) {
      final Class[] keys = this.keys;
      final int mask = keys.length - 1;
      final AdvancedExternalizer[] values = this.values;
      Class k;
      int hc = System.identityHashCode(key) & mask;
      for (int idx = hc;; idx = hc++ & mask) {
         k = keys[idx];
         if (k == null) {
            keys[idx] = key;
            values[idx] = value;
            if (++count > resizeCount) {
               resize();
            }
            return;
         }
         if (k == key) {
            values[idx] = value;
            return;
         }
      }
   }

   private void resize() {
      final Class[] oldKeys = keys;
      final int oldsize = oldKeys.length;
      final AdvancedExternalizer[] oldValues = values;
      if (oldsize >= 0x40000000) {
         throw new IllegalStateException("Table full");
      }
      final int newsize = oldsize << 1;
      final int mask = newsize - 1;
      final Class[] newKeys = new Class[newsize];
      final AdvancedExternalizer[] newValues = new AdvancedExternalizer[newsize];
      keys = newKeys;
      values = newValues;
      if ((resizeCount <<= 1) == 0) {
         resizeCount = Integer.MAX_VALUE;
      }
      for (int oi = 0; oi < oldsize; oi ++) {
         final Class key = oldKeys[oi];
         if (key != null) {
            int ni = System.identityHashCode(key) & mask;
            for (;;) {
               final Object v = newKeys[ni];
               if (v == null) {
                  // found
                  newKeys[ni] = key;
                  newValues[ni] = oldValues[oi];
                  break;
               }
               ni = (ni + 1) & mask;
            }
         }
      }
   }

   public void clear() {
      Arrays.fill(keys, null);
      Arrays.fill(values, null);
      count = 0;
   }

   /**
    * Get a string summary representation of this map.
    *
    * @return a string representation
    */
   public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Map length = ").append(keys.length).append(", count = ").append(count).append(", resize count = ").append(resizeCount).append('\n');
      for (int i = 0; i < keys.length; i ++) {
         builder.append('[').append(i).append("] = ");
         if (keys[i] != null) {
            final int hc = System.identityHashCode(keys[i]);
            builder.append("{ ").append(keys[i]).append(" (hash ").append(hc).append(", modulus ").append(hc % keys.length).append(") => ").append(values[i]).append(" }");
         } else {
            builder.append("(blank)");
         }
         builder.append('\n');
      }
      return builder.toString();
   }

   public IdToExternalizerMap reverseMap() {
      IdToExternalizerMap reverse = new IdToExternalizerHashMap(8, loadFactor);
      fillReverseMap(reverse);
      return reverse;
   }

   public IdToExternalizerMap reverseMap(int maxId) {
      // If max identifier is known, provide a single array backed map
      IdToExternalizerMap reverse = new IdToExternalizerArrayMap(maxId);
      fillReverseMap(reverse);
      return reverse;
   }

   private void fillReverseMap(IdToExternalizerMap reverse) {
      for (AdvancedExternalizer ext : values) {
         if (ext != null) {
            AdvancedExternalizer prev = reverse.get(ext.getId());
            if (prev != null && !prev.equals(ext))
               throw log.duplicateExternalizerIdFound(
                     ext.getId(), prev.getClass().getName());

            reverse.put(ext.getId(), ext);
         }
      }
   }

   interface IdToExternalizerMap {
      AdvancedExternalizer get(int key);
      void put(int key, AdvancedExternalizer value);
      void clear();
   }

   private static final class IdToExternalizerArrayMap implements IdToExternalizerMap {
      private AdvancedExternalizer[] values;
      private int count;

      private IdToExternalizerArrayMap(int initialCapacity) {
         if (initialCapacity < 1) {
            throw new IllegalArgumentException("initialCapacity must be > 0");
         }
         if (initialCapacity < 16) {
            initialCapacity = 16;
         } else {
            // round up
            final int c = Integer.highestOneBit(initialCapacity) - 1;
            initialCapacity = Integer.highestOneBit(initialCapacity + c);
         }
         values = new AdvancedExternalizer[initialCapacity];
      }

      /**
       * Get a value from the map.
       *
       * @param key the key
       * @return the map value at the given key, or null if it's not found
       */
      public AdvancedExternalizer get(int key) {
         final AdvancedExternalizer[] values = this.values;
         return values[key];
      }

      /**
       * Put a value into the map.  Any previous mapping is discarded silently.
       *
       * @param key the key
       * @param value the value to store
       */
      public void put(int key, AdvancedExternalizer value) {
         final AdvancedExternalizer[] values = this.values;
         if (values[key] == null) {
            values[key] = value;
            if (++count > values.length)
               resize();
         }
      }

      private void resize() {
         final AdvancedExternalizer[] oldValues = values;
         final int oldsize = oldValues.length;
         if (oldsize >= 0x40000000) {
            throw new IllegalStateException("Table full");
         }
         final int newsize = oldsize << 1;
         final AdvancedExternalizer[] newValues = new AdvancedExternalizer[newsize];
         values = newValues;
         for (int oi = 0; oi < oldsize; oi ++) {
            if (oldValues[oi] != null) {
               newValues[oi] = oldValues[oi];
            }
         }
      }

      public void clear() {
         Arrays.fill(values, null);
         count = 0;
      }

      /**
       * Get a string summary representation of this map.
       *
       * @return a string representation
       */
      public String toString() {
         StringBuilder builder = new StringBuilder();
         builder.append("Map length = ").append(values.length).append(", count = ").append(count).append('\n');
         for (int i = 0; i < values.length; i ++) {
            builder.append('[').append(i).append("] = ");
            if (values[i] != null) {
               builder.append("{ ").append(values[i]).append("}");
            } else {
               builder.append("(blank)");
            }
            builder.append('\n');
         }
         return builder.toString();
      }

   }

   private static final class IdToExternalizerHashMap implements IdToExternalizerMap {
      private AdvancedExternalizer[] values;
      private int[] keys;
      private int count;
      private int resizeCount;

      private IdToExternalizerHashMap(int initialCapacity, final float loadFactor) {
         if (initialCapacity < 1) {
            throw new IllegalArgumentException("initialCapacity must be > 0");
         }
         if (loadFactor <= 0.0f || loadFactor >= 1.0f) {
            throw new IllegalArgumentException("loadFactor must be > 0.0 and < 1.0");
         }
         if (initialCapacity < 16) {
            initialCapacity = 16;
         } else {
            // round up
            final int c = Integer.highestOneBit(initialCapacity) - 1;
            initialCapacity = Integer.highestOneBit(initialCapacity + c);
         }
         keys = new int[initialCapacity];
         values = new AdvancedExternalizer[initialCapacity];
         resizeCount = (int) ((double) initialCapacity * (double) loadFactor);
      }

      /**
       * Get a value from the map.
       *
       * @param key the key
       * @return the map value at the given key, or null if it's not found
       */
      public AdvancedExternalizer get(int key) {
         final int[] keys = this.keys;
         final AdvancedExternalizer[] values = this.values;
         final int mask = keys.length - 1;
         int hc = key & mask;
         int k;
         for (;;) {
            k = keys[hc];
            if (k == key) {
               return values[hc];
            }
            if (values[hc] == null) {
               // not found
               return null;
            }
            hc = (hc + 1) & mask;
         }
      }

      /**
       * Put a value into the map.  Any previous mapping is discarded silently.
       *
       * @param key the key
       * @param value the value to store
       */
      public void put(int key, AdvancedExternalizer value) {
         final int[] keys = this.keys;
         final int mask = keys.length - 1;
         final AdvancedExternalizer[] values = this.values;
         int k;
         int hc = key & mask;
         for (int idx = hc;; idx = hc++ & mask) {
            k = keys[idx];
            if (values[idx] == null) {
               keys[idx] = key;
               values[idx] = value;
               if (++count > resizeCount) {
                  resize();
               }
               return;
            }
            if (k == key) {
               values[idx] = value;
               return;
            }
         }
      }

      private void resize() {
         final int[] oldKeys = keys;
         final int oldsize = oldKeys.length;
         final AdvancedExternalizer[] oldValues = values;
         if (oldsize >= 0x40000000) {
            throw new IllegalStateException("Table full");
         }
         final int newsize = oldsize << 1;
         final int mask = newsize - 1;
         final int[] newKeys = new int[newsize];
         final AdvancedExternalizer[] newValues = new AdvancedExternalizer[newsize];
         keys = newKeys;
         values = newValues;
         if ((resizeCount <<= 1) == 0) {
            resizeCount = Integer.MAX_VALUE;
         }
         for (int oi = 0; oi < oldsize; oi ++) {
            final int key = oldKeys[oi];
            if (oldValues[oi] != null) {
               int ni = key & mask;
               for (;;) {
                  if (newValues[ni] == null) {
                     // found
                     newKeys[ni] = key;
                     newValues[ni] = oldValues[oi];
                     break;
                  }
                  ni = (ni + 1) & mask;
               }
            }
         }
      }

      public void clear() {
         Arrays.fill(keys, 0);
         count = 0;
      }

      /**
       * Get a string summary representation of this map.
       *
       * @return a string representation
       */
      public String toString() {
         StringBuilder builder = new StringBuilder();
         builder.append("Map length = ").append(keys.length).append(", count = ").append(count).append(", resize count = ").append(resizeCount).append('\n');
         for (int i = 0; i < keys.length; i ++) {
            builder.append('[').append(i).append("] = ");
            if (values[i] != null) {
               final int hc = keys[i];
               builder.append("{ ").append(keys[i]).append(" (hash ").append(hc).append(", modulus ").append(hc % keys.length).append(") => ").append(values[i]).append(" }");
            } else {
               builder.append("(blank)");
            }
            builder.append('\n');
         }
         return builder.toString();
      }

   }

}
