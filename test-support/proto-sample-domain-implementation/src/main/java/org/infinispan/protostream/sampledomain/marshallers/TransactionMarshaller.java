package org.infinispan.protostream.sampledomain.marshallers;

import java.io.IOException;
import java.util.Date;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.sampledomain.Transaction;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class TransactionMarshaller implements MessageMarshaller<Transaction> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Transaction";
   }

   @Override
   public Class<Transaction> getJavaClass() {
      return Transaction.class;
   }

   @Override
   public Transaction readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      String longDescription = reader.readString("longDescription");
      String notes = reader.readString("notes");
      int accountId = reader.readInt("accountId");
      Date date = reader.readDate("date");
      double amount = reader.readDouble("amount");
      boolean isDebit = reader.readBoolean("isDebit");
      boolean isValid = reader.readBoolean("isValid");

      Transaction transaction = new Transaction();
      transaction.setId(id);
      transaction.setDescription(description);
      transaction.setLongDescription(longDescription);
      transaction.setNotes(notes);
      transaction.setAccountId(accountId);
      transaction.setDate(date);
      transaction.setAmount(amount);
      transaction.setDebit(isDebit);
      transaction.setValid(isValid);
      return transaction;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Transaction transaction) throws IOException {
      writer.writeInt("id", transaction.getId());
      writer.writeString("description", transaction.getDescription());
      writer.writeString("longDescription", transaction.getLongDescription());
      writer.writeString("notes", transaction.getNotes());
      writer.writeInt("accountId", transaction.getAccountId());
      writer.writeLong("date", transaction.getDate().getTime());
      writer.writeDouble("amount", transaction.getAmount());
      writer.writeBoolean("isDebit", transaction.isDebit());
      writer.writeBoolean("isValid", transaction.isValid());
   }
}
