package org.infinispan.server.test.dataconversion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.commons.marshall.SerializeWith;

/**
 * @since 9.4
 */
@SerializeWith(value = Currency.Externalizer.class)
public class Currency implements Serializable {

   private final String country;
   private final String symbol;

   public Currency(String country, String symbol) {
      this.country = country;
      this.symbol = symbol;
   }

   public String getCountry() {
      return country;
   }

   public String getSymbol() {
      return symbol;
   }

   public static final class Externalizer implements org.infinispan.commons.marshall.Externalizer<Currency> {
      @Override
      public void writeObject(ObjectOutput output, Currency currency) throws IOException {
         output.writeUTF(currency.country);
         output.writeUTF(currency.symbol);
      }

      @Override
      public Currency readObject(ObjectInput input) throws IOException {
         return new Currency(input.readUTF(), input.readUTF());
      }
   }
}
