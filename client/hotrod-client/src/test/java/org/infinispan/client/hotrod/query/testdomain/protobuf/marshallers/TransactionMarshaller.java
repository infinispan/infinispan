package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;
import java.util.Date;

import org.infinispan.client.hotrod.query.testdomain.protobuf.TransactionPB;
import org.infinispan.protostream.MessageMarshaller;

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
      String longDescription = reader.readString("longDescription");
      String notes = reader.readString("notes");
      int accountId = reader.readInt("accountId");
      long date = reader.readLong("date");
      double amount = reader.readDouble("amount");
      boolean isDebit = reader.readBoolean("isDebit");
      boolean isValid = reader.readBoolean("isValid");

      TransactionPB transaction = new TransactionPB();
      transaction.setId(id);
      transaction.setDescription(description);
      transaction.setLongDescription(longDescription);
      transaction.setNotes(notes);
      transaction.setAccountId(accountId);
      transaction.setDate(new Date(date));
      transaction.setAmount(amount);
      transaction.setDebit(isDebit);
      transaction.setValid(isValid);
      return transaction;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, TransactionPB transaction) throws IOException {
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
