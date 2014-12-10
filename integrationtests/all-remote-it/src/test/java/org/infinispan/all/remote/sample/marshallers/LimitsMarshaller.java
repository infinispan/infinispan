package org.infinispan.all.remote.sample.marshallers;

import org.infinispan.all.remote.sample.classes.Account;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 */
public class LimitsMarshaller implements MessageMarshaller<Account.Limits> {
   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Limits";
   }
   @Override
   public Class<? extends Account.Limits> getJavaClass() {
      return Account.Limits.class;
   }
   @Override
   public Account.Limits readFrom(ProtoStreamReader reader) throws IOException {
      double maxDailyLimit = reader.readDouble("maxDailyLimit");
      double maxTransactionLimit = reader.readDouble("maxTransactionLimit");
      Account.Limits account = new Account.Limits();
      account.setMaxDailyLimit(maxDailyLimit);
      account.setMaxTransactionLimit(maxTransactionLimit);
      return account;
   }
   @Override
   public void writeTo(ProtoStreamWriter writer, Account.Limits limits) throws IOException {
      writer.writeDouble("maxDailyLimit", limits.getMaxDailyLimit());
      writer.writeDouble("maxTransactionLimit", limits.getMaxTransactionLimit());
   }
}