package org.infinispan.protostream.sampledomain.marshallers;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.sampledomain.Account;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class LimitsMarshaller implements MessageMarshaller<Account.Limits> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Limits";
   }

   @Override
   public Class<Account.Limits> getJavaClass() {
      return Account.Limits.class;
   }

   @Override
   public Account.Limits readFrom(ProtoStreamReader reader) throws IOException {
      double maxDailyLimit = reader.readDouble("maxDailyLimit");
      double maxTransactionLimit = reader.readDouble("maxTransactionLimit");
      String[] payees = reader.readArray("payees", String.class);

      Account.Limits limits = new Account.Limits();
      limits.setMaxDailyLimit(maxDailyLimit);
      limits.setMaxTransactionLimit(maxTransactionLimit);
      limits.setPayees(payees);
      return limits;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Account.Limits limits) throws IOException {
      writer.writeDouble("maxDailyLimit", limits.getMaxDailyLimit());
      writer.writeDouble("maxTransactionLimit", limits.getMaxTransactionLimit());
      writer.writeArray("payees", limits.getPayees(), String.class);
   }
}
