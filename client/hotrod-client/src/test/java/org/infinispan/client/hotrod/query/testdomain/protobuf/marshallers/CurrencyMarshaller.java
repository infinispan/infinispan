package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Account;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class CurrencyMarshaller implements EnumMarshaller<Account.Currency> {

   @Override
   public Class<Account.Currency> getJavaClass() {
      return Account.Currency.class;
   }

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Currency";
   }

   @Override
   public Account.Currency decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return Account.Currency.EUR;
         case 1:
            return Account.Currency.GBP;
         case 2:
            return Account.Currency.USD;
         case 3:
            return Account.Currency.BRL;
      }
      return null;  // unknown value
   }

   @Override
   public int encode(Account.Currency currency) {
      switch (currency) {
         case EUR:
            return 0;
         case GBP:
            return 1;
         case USD:
            return 2;
         case BRL:
            return 3;
         default:
            throw new IllegalArgumentException("Unexpected Account.Currency value : " + currency);
      }
   }
}
