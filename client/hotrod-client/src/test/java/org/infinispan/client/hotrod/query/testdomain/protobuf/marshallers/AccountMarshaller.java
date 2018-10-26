package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.LimitsPB;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Limits;

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
      Limits limits = reader.readObject("limits", LimitsPB.class);
      Limits hardLimits = reader.readObject("hardLimits", LimitsPB.class);
      List<byte[]> blurb = reader.readCollection("blurb", new ArrayList<>(), byte[].class);
      Account.Currency[] currencies = reader.readArray("currencies", Account.Currency.class);

      AccountPB account = new AccountPB();
      account.setId(id);
      account.setDescription(description);
      account.setCreationDate(new Date(creationDate));
      account.setLimits(limits);
      account.setHardLimits(hardLimits);
      account.setBlurb(blurb);
      account.setCurrencies(currencies);
      return account;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AccountPB account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeDate("creationDate", account.getCreationDate());
      writer.writeObject("limits", account.getLimits(), LimitsPB.class);
      writer.writeObject("hardLimits", account.getHardLimits(), LimitsPB.class);
      writer.writeCollection("blurb", account.getBlurb(), byte[].class);
      writer.writeArray("currencies", account.getCurrencies(), Account.Currency.class);
   }
}
