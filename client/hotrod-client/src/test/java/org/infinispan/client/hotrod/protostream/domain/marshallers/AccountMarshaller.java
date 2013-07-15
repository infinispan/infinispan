package org.infinispan.client.hotrod.protostream.domain.marshallers;

import org.infinispan.client.hotrod.protostream.domain.Account;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 */
public class AccountMarshaller implements MessageMarshaller<Account> {

   @Override
   public String getFullName() {
      return "sample_bank_account.Account";
   }

   @Override
   public Account readFrom(ProtobufReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");

      Account account = new Account();
      account.setId(id);
      account.setDescription(description);
      return account;
   }

   @Override
   public void writeTo(ProtobufWriter writer, Account account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
   }
}
