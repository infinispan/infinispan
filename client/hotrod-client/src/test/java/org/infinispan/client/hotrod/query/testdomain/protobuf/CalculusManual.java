package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigInteger;

public class CalculusManual {

   private BigInteger purchases;

   public CalculusManual(BigInteger purchases) {
      this.purchases = purchases;
   }

   public BigInteger getPurchases() {
      return purchases;
   }
}
