package org.infinispan.client.hotrod.marshall;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;

import java.io.IOException;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class EmbeddedAccountMarshaller implements MessageMarshaller<AccountHS> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account";
   }

   @Override
   public Class<AccountHS> getJavaClass() {
      return AccountHS.class;
   }

   @Override
   public AccountHS readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      long creationDate = reader.readLong("creationDate");

      AccountHS account = new AccountHS();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(new Date(creationDate));
      return account;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AccountHS account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeLong("creationDate", account.getCreationDate().getTime());
   }
}
