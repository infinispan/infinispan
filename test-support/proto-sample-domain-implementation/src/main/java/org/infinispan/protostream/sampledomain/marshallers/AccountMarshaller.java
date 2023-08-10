package org.infinispan.protostream.sampledomain.marshallers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.sampledomain.Account;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class AccountMarshaller implements MessageMarshaller<Account> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account";
   }

   @Override
   public Class<Account> getJavaClass() {
      return Account.class;
   }

   @Override
   public Account readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      String description = reader.readString("description");
      Date creationDate = reader.readDate("creationDate");
      Account.Limits limits = reader.readObject("limits", Account.Limits.class);
      Account.Limits hardLimits = reader.readObject("hardLimits", Account.Limits.class);
      List<byte[]> blurb = reader.readCollection("blurb", new ArrayList<>(), byte[].class);
      Account.Currency[] currencies = reader.readArray("currencies", Account.Currency.class);

      Account account = new Account();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(creationDate);
      account.setLimits(limits);
      account.setHardLimits(hardLimits);
      account.setBlurb(blurb);
      account.setCurrencies(currencies);
      return account;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Account account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeDate("creationDate", account.getCreationDate());
      writer.writeObject("limits", account.getLimits(), Account.Limits.class);
      writer.writeObject("hardLimits", account.getHardLimits(), Account.Limits.class);
      writer.writeCollection("blurb", account.getBlurb(), byte[].class);
      writer.writeArray("currencies", account.getCurrencies(), Account.Currency.class);
   }
}
