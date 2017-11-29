package org.infinispan.server.test.task.servertask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

@SuppressWarnings("unused")
@SerializeWith(SpotPrice.Serializer.class)
public class SpotPrice {

   private final String ticker;
   private final Instant timestamp;
   private final Float priceUSD;

   public SpotPrice(String ticker, Instant timestamp, Float priceUSD) {
      this.ticker = ticker;
      this.timestamp = timestamp;
      this.priceUSD = priceUSD;
   }

   public String getTicker() {
      return ticker;
   }

   public Instant getTimestamp() {
      return timestamp;
   }

   public Float getPriceUSD() {
      return priceUSD;
   }

   public static class Serializer implements Externalizer<SpotPrice> {

      @Override
      public void writeObject(ObjectOutput output, SpotPrice object) throws IOException {
         output.writeUTF(object.ticker);
         output.writeObject(object.timestamp);
         output.writeObject(object.priceUSD);
      }

      @Override
      public SpotPrice readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String ticker = input.readUTF();
         Instant timestamp = (Instant) input.readObject();
         Float priceUSD = (Float) input.readObject();
         return new SpotPrice(ticker, timestamp, priceUSD);
      }
   }
}
