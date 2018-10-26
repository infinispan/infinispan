package org.infinispan.client.hotrod.marshall;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.LimitsHS;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class EmbeddedLimitsMarshaller implements MessageMarshaller<LimitsHS> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Limits";
   }

   @Override
   public Class<LimitsHS> getJavaClass() {
      return LimitsHS.class;
   }

   @Override
   public LimitsHS readFrom(ProtoStreamReader reader) throws IOException {
      double maxDailyLimit = reader.readDouble("maxDailyLimit");
      double maxTransactionLimit = reader.readDouble("maxTransactionLimit");
      String[] payees = reader.readArray("payees", String.class);

      LimitsHS limits = new LimitsHS();
      limits.setMaxDailyLimit(maxDailyLimit);
      limits.setMaxTransactionLimit(maxTransactionLimit);
      limits.setPayees(payees);
      return limits;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, LimitsHS limits) throws IOException {
      writer.writeDouble("maxDailyLimit", limits.getMaxDailyLimit());
      writer.writeDouble("maxTransactionLimit", limits.getMaxTransactionLimit());
      writer.writeArray("payees", limits.getPayees(), String.class);
   }
}
