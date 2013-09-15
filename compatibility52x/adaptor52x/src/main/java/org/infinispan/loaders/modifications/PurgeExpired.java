package org.infinispan.loaders.modifications;

public class PurgeExpired implements Modification {
   @Override
   public Type getType() {
      return Type.PURGE_EXPIRED;
   }
}
