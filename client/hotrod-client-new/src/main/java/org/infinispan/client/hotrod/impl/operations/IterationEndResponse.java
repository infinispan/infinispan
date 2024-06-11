package org.infinispan.client.hotrod.impl.operations;

/**
 * @author gustavonalle
 * @since 8.0
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
