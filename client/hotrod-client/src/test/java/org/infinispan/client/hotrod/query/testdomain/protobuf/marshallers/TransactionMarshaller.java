package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.client.hotrod.query.testdomain.protobuf.TransactionPB;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class TransactionMarshaller implements MessageMarshaller<TransactionPB> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Transaction";
   }

   @Override
   public Class<TransactionPB> getJavaClass() {
      return TransactionPB.class;
   }

   @Override
   public TransactionPB readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      int accountId = reader.readInt("accountId");
      long date = reader.readLong("date");
      double amount = reader.readDouble("amount");
      boolean isDebit = reader.readBoolean("isDebit");

      TransactionPB transaction = new TransactionPB();
      transaction.setId(id);
      transaction.setDescription(description);
      transaction.setAccountId(accountId);
      transaction.setDate(new Date(date));
      transaction.setAmount(amount);
      transaction.setDebit(isDebit);
      return transaction;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, TransactionPB transaction) throws IOException {
      writer.writeInt("id", transaction.getId());
      writer.writeString("description", transaction.getDescription());
      writer.writeInt("accountId", transaction.getAccountId());
      writer.writeLong("date", transaction.getDate().getTime());
      writer.writeDouble("amount", transaction.getAmount());
      writer.writeBoolean("isDebit", transaction.isDebit());
   }
}
