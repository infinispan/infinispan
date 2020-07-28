package org.infinispan.util;

import java.util.Objects;

import org.infinispan.Cache;

/**
 * @author Pedro Ruivo
 * @since 12.0
 */
public enum TestOperation {
   PUT {
      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         cache.put(key, newValue);
         return newValue;
      }
   },
   PUT_IF_ABSENT {
      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         V result = cache.putIfAbsent(key, newValue);
         return result == null ? newValue : result;
      }
   },
   REPLACE {
      @Override
      public boolean requiresPreviousValue() {
         return true;
      }

      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         V result = cache.replace(key, newValue);
         return Objects.equals(result, prevValue) ? newValue : result;
      }
   },
   REPLACE_CONDITIONAL {
      @Override
      public boolean requiresPreviousValue() {
         return true;
      }

      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         boolean result = cache.replace(key, prevValue, newValue);
         return result ? newValue : prevValue;
      }
   },
   REMOVE {
      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         cache.remove(key);
         return null;
      }
   },
   REMOVE_CONDITIONAL {
      @Override
      public boolean requiresPreviousValue() {
         return true;
      }

      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         boolean result = cache.remove(key, prevValue);
         return result ? null : prevValue;
      }
   };

   public boolean requiresPreviousValue() {
      return false;
   }

   public abstract <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue);
}
