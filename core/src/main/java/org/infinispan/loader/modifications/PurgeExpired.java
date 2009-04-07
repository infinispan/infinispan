package org.infinispan.loader.modifications;

public class PurgeExpired implements Modification {
   public Type getType() {
      return Type.PURGE_EXPIRED;
   }
}
