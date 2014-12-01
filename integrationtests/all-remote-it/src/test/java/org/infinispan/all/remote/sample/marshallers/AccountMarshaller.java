package org.infinispan.all.remote.sample.marshallers;

import org.infinispan.all.remote.sample.classes.Account;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author anistor@redhat.com
 */
public class AccountMarshaller implements MessageMarshaller<Account> {
   @Override
   public String getTypeName() {
      return "sample_bank_account.Account";
   }
   @Override
   public Class<? extends Account> getJavaClass() {
      return Account.class;
   }
   @Override
   public Account readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      long creationDate = reader.readLong("creationDate");
      Account.Limits limits = reader.readObject("limits", Account.Limits.class);
      List<byte[]> blurb = reader.readCollection("blurb", new ArrayList<byte[]>(), byte[].class);
      Account account = new Account();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(new Date(creationDate));
      account.setLimits(limits);
      account.setBlurb(blurb);
      return account;
   }
   @Override
   public void writeTo(ProtoStreamWriter writer, Account account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeLong("creationDate", account.getCreationDate().getTime());
      writer.writeObject("limits", account.getLimits(), Account.Limits.class);
      writer.writeCollection("blurb", account.getBlurb(), byte[].class);
   }
}