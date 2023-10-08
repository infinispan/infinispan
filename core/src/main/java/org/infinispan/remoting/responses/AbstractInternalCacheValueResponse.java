package org.infinispan.remoting.responses;

import java.util.Objects;

import org.infinispan.container.entries.InternalCacheValue;

abstract class AbstractInternalCacheValueResponse implements SuccessfulResponse<InternalCacheValue<?>> {

   protected InternalCacheValue<?> icv;

   public AbstractInternalCacheValueResponse(InternalCacheValue<?> icv) {
      this.icv = icv;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      AbstractInternalCacheValueResponse that = (AbstractInternalCacheValueResponse) o;
      return Objects.equals(icv, that.icv);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(icv);
   }
}
