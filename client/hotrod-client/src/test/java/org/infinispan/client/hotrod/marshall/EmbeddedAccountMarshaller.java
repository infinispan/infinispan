package org.infinispan.client.hotrod.marshall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Limits;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.LimitsHS;

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
      Limits limits = reader.readObject("limits", LimitsHS.class);
      Limits hardLimits = reader.readObject("hardLimits", LimitsHS.class);
      List<byte[]> blurb = reader.readCollection("blurb", new ArrayList<>(), byte[].class);
      Account.Currency[] currencies = reader.readArray("currencies", Account.Currency.class);

      AccountHS account = new AccountHS();
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
   public void writeTo(ProtoStreamWriter writer, AccountHS account) throws IOException {
      writer.writeInt("id", account.getId());
      writer.writeString("description", account.getDescription());
      writer.writeLong("creationDate", account.getCreationDate().getTime());
      writer.writeObject("limits", account.getLimits(), LimitsHS.class);
      writer.writeObject("hardLimits", account.getHardLimits(), LimitsHS.class);
      writer.writeCollection("blurb", account.getBlurb(), byte[].class);
      writer.writeArray("currencies", account.getCurrencies(), Account.Currency.class);
   }
}
