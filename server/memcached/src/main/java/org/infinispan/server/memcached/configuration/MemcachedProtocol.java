package org.infinispan.server.memcached.configuration;

/**
 * @since 15.0
 **/
public enum MemcachedProtocol {
   AUTO(true, true),
   TEXT(true, false),
   BINARY(false, true),
   ;

   private final boolean text;
   private final boolean binary;

   MemcachedProtocol(boolean text, boolean binary) {
      this.text = text;
      this.binary = binary;
   }

   public boolean isBinary() {
      return binary;
   }

   public boolean isText() {
      return text;
   }
}
