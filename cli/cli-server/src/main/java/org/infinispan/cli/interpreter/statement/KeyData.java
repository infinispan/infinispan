package org.infinispan.cli.interpreter.statement;

public class KeyData {
   final String cacheName;
   final Object key;

   public KeyData(final String cacheName, final Object key) {
      this.cacheName = cacheName;
      this.key = key;
   }

   public KeyData(final Object key) {
      this(null, key);
   }

   public String getCacheName() {
      return cacheName;
   }

   public Object getKey() {
      return key;
   }

}
