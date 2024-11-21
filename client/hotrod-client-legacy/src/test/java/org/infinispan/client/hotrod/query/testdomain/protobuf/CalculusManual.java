package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigInteger;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class CalculusManual {

   @ProtoField(1)
   final BigInteger purchases;

   @ProtoFactory
   public CalculusManual(BigInteger purchases) {
      this.purchases = purchases;
   }

   public BigInteger getPurchases() {
      return purchases;
   }
}
