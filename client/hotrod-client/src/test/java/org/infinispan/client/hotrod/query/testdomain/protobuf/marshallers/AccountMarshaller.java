package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AccountMarshaller implements MessageMarshaller<AccountPB> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account";
   }

   @Override
   public Class<AccountPB> getJavaClass() {
      return AccountPB.class;
   }

   @Override
   public AccountPB readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      long creationDate = reader.readLong("creationDate");

      AccountPB account = new AccountPB();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(new Date(creationDate));
      return account;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AccountPB account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeLong("creationDate", account.getCreationDate().getTime());
   }
}
