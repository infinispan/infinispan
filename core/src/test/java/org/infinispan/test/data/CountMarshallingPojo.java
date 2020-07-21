package org.infinispan.test.data;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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

   private static final Map<String, Integer> marshallCount = new ConcurrentHashMap<>();
   private static final Map<String, Integer> unmarshallCount = new ConcurrentHashMap<>();

   private String name;
   private int value;

   public static void reset(String name) {
      marshallCount.put(name, 0);
      unmarshallCount.put(name, 0);
   }

   public static int getMarshallCount(String name) {
      return marshallCount.getOrDefault(name, 0);
   }

   public static int getUnmarshallCount(String name) {
      return unmarshallCount.getOrDefault(name, 0);
   }

   CountMarshallingPojo() {
   }

   public CountMarshallingPojo(String name, int value) {
      this.name = name;
      this.value = value;
   }

   @ProtoField(number = 1, defaultValue = "0")
   int getValue() {
      return value;
   }

   void setValue(int i) {
      this.value = i;
   }

   @ProtoField(number = 2)
   String getName() {
      int serCount = marshallCount.merge(name, 1, Integer::sum);
      log.trace("marshallCount=" + serCount);
      return name;
   }

   void setName(String name) {
      this.name = name;
      int deserCount = unmarshallCount.merge(this.name, 1, Integer::sum);
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
             "name=" + name +
             ", value=" + value +
             '}';
   }
}
