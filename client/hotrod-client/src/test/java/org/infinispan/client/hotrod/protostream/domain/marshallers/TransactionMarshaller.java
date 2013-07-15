package org.infinispan.client.hotrod.protostream.domain.marshallers;

import org.infinispan.client.hotrod.protostream.domain.Transaction;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.Date;

/**
 * @author anistor@redhat.com
 */
public class TransactionMarshaller implements MessageMarshaller<Transaction> {

   @Override
   public String getFullName() {
      return "sample_bank_account.Transaction";
   }

   @Override
   public Transaction readFrom(ProtobufReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      int accountId = reader.readInt("accountId");
      long date = reader.readLong("date");
      double amount = reader.readDouble("amount");
      boolean isDebit = reader.readBoolean("debit");

      Transaction transaction = new Transaction();
      transaction.setId(id);
      transaction.setDescription(description);
      transaction.setAccountId(accountId);
      transaction.setDate(new Date(date));
      transaction.setAmount(amount);
      transaction.setDebit(isDebit);
      return transaction;
   }

   @Override
   public void writeTo(ProtobufWriter writer, Transaction transaction) throws IOException {
      writer.writeInt("id", transaction.getId());
      writer.writeString("description", transaction.getDescription());
      writer.writeInt("accountId", transaction.getAccountId());
      writer.writeLong("date", transaction.getDate());
      writer.writeDouble("amount", transaction.getAmount());
      writer.writeBoolean("debit", transaction.isDebit());
   }
}
