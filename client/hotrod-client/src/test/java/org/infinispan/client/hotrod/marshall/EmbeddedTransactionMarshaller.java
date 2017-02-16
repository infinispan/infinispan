package org.infinispan.client.hotrod.marshall;

import java.io.IOException;
import java.util.Date;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
public class EmbeddedTransactionMarshaller implements MessageMarshaller<TransactionHS> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Transaction";
   }

   @Override
   public Class<TransactionHS> getJavaClass() {
      return TransactionHS.class;
   }

   @Override
   public TransactionHS readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      int accountId = reader.readInt("accountId");
      long date = reader.readLong("date");
      double amount = reader.readDouble("amount");
      boolean isDebit = reader.readBoolean("isDebit");
      boolean isValid = reader.readBoolean("isValid");

      TransactionHS transaction = new TransactionHS();
      transaction.setId(id);
      transaction.setDescription(description);
      transaction.setAccountId(accountId);
      transaction.setDate(new Date(date));
      transaction.setAmount(amount);
      transaction.setDebit(isDebit);
      transaction.setValid(isValid);
      return transaction;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, TransactionHS transaction) throws IOException {
      writer.writeInt("id", transaction.getId());
      writer.writeString("description", transaction.getDescription());
      writer.writeInt("accountId", transaction.getAccountId());
      writer.writeLong("date", transaction.getDate().getTime());
      writer.writeDouble("amount", transaction.getAmount());
      writer.writeBoolean("isDebit", transaction.isDebit());
      writer.writeBoolean("isValid", transaction.isValid());
   }
}
