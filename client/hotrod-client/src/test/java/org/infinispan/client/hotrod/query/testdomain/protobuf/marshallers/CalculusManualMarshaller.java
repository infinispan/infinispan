package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;
import java.math.BigInteger;

import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusManual;
import org.infinispan.protostream.MessageMarshaller;

public class CalculusManualMarshaller implements MessageMarshaller<CalculusManual> {

   @Override
   public CalculusManual readFrom(ProtoStreamReader reader) throws IOException {
      BigInteger purchases = reader.readObject("purchases", BigInteger.class);
      return new CalculusManual(purchases);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, CalculusManual calculus) throws IOException {
      writer.writeObject("purchases", calculus.getPurchases(), BigInteger.class);
   }

   @Override
   public Class<? extends CalculusManual> getJavaClass() {
      return CalculusManual.class;
   }

   @Override
   public String getTypeName() {
      return "lab.manual.Calculus";
   }
}
