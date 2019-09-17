package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Currency;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class CurrencyMarshaller implements EnumMarshaller<Currency> {

   @Override
   public Class<Currency> getJavaClass() {
      return Currency.class;
   }

   @Override
   public String getTypeName() {
      return "sample_bank_account.Account.Currency";
   }

   @Override
   public Currency decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return Currency.EUR;
         case 1:
            return Currency.GBP;
         case 2:
            return Currency.USD;
         case 3:
            return Currency.BRL;
      }
      return null;  // unknown value
   }

   @Override
   public int encode(Currency currency) {
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
