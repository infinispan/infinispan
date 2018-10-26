package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;

import org.infinispan.client.hotrod.query.testdomain.protobuf.LimitsPB;
import org.infinispan.protostream.MessageMarshaller;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class LimitsMarshaller implements MessageMarshaller<LimitsPB> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Limits";
   }

   @Override
   public Class<LimitsPB> getJavaClass() {
      return LimitsPB.class;
   }

   @Override
   public LimitsPB readFrom(ProtoStreamReader reader) throws IOException {
      double maxDailyLimit = reader.readDouble("maxDailyLimit");
      double maxTransactionLimit = reader.readDouble("maxTransactionLimit");
      String[] payees = reader.readArray("payees", String.class);

      LimitsPB limits = new LimitsPB();
      limits.setMaxDailyLimit(maxDailyLimit);
      limits.setMaxTransactionLimit(maxTransactionLimit);
      limits.setPayees(payees);
      return limits;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, LimitsPB limits) throws IOException {
      writer.writeDouble("maxDailyLimit", limits.getMaxDailyLimit());
      writer.writeDouble("maxTransactionLimit", limits.getMaxTransactionLimit());
      writer.writeArray("payees", limits.getPayees(), String.class);
   }
}
