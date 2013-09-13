package org.infinispan.client.hotrod.marshall;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.Date;

/**
 * @author anistor@redhat.com
 */
public class EmbeddedAccountMarshaller implements MessageMarshaller<EmbeddedAccount> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account";
   }

   @Override
   public Class<? extends EmbeddedAccount> getJavaClass() {
      return EmbeddedAccount.class;
   }

   @Override
   public EmbeddedAccount readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      long creationDate = reader.readLong("creationDate");

      EmbeddedAccount account = new EmbeddedAccount();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(new Date(creationDate));
      return account;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, EmbeddedAccount account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeLong("creationDate", account.getCreationDate().getTime());
   }
}
