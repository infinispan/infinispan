package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Status;

abstract class AbstractFunctionalMap<K, V> implements FunctionalMap<K, V> {

   protected final FunctionalMapImpl<K, V> fmap;

   protected AbstractFunctionalMap(FunctionalMapImpl<K, V> fmap) {
      this.fmap = fmap;
   }

   @Override
   public String getName() {
      return "";
   }

   @Override
   public Status getStatus() {
      return fmap.getStatus();
   }

   @Override
   public void close() throws Exception {
      fmap.close();
   }

}
