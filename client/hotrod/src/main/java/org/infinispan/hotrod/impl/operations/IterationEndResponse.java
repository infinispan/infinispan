package org.infinispan.hotrod.impl.operations;

/**
 * @since 14.0
 */
public class IterationEndResponse {

   private final short status;

   public IterationEndResponse(short status) {
      this.status = status;
   }

   public short getStatus() {
      return status;
   }
}
