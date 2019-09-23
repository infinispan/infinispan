package org.infinispan.test.data;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A Pojo that records how many times it has been marshalled/unmarshalled based upon the number of calls to its
 * annotated setter/getter methods via the generated Protostream marhaller. Test instances should never attempt to call
 * the setter/getter methods directly as this will result in unexpected counts.
 */
public class CountMarshallingPojo {
   private static final Log log = LogFactory.getLog(CountMarshallingPojo.class);

   private static AtomicInteger marshallCount = new AtomicInteger();
   private static AtomicInteger unmarshallCount = new AtomicInteger();
   private long value;

   public static void reset() {
      marshallCount.set(0);
      unmarshallCount.set(0);
   }

   public static int getMarshallCount() {
      return marshallCount.get();
   }

   public static int getUnmarshallCount() {
      return unmarshallCount.get();
   }

   public CountMarshallingPojo() {
   }

   public CountMarshallingPojo(long value) {
      this.value = value;
   }

   @ProtoField(number = 1, defaultValue = "0")
   long getValue() {
      int serCount = marshallCount.incrementAndGet();
      log.trace("marshallCount=" + serCount);
      return value;
   }

   void setValue(long i) {
      this.value = i;
      int deserCount = unmarshallCount.incrementAndGet();
      log.trace("unmarshallCount=" + deserCount);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CountMarshallingPojo that = (CountMarshallingPojo) o;
      return value == that.value;
   }

   @Override
   public int hashCode() {
      return Objects.hash(value);
   }

   @Override
   public String toString() {
      return "CountMarshallingPojo{" +
            "value=" + value +
            '}';
   }
}
