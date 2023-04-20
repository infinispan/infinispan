package org.infinispan.server.functional.extensions.entities;

import java.util.Objects;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.server.functional.extensions.filters.DynamicCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.DynamicConverterFactory;
import org.infinispan.server.functional.extensions.filters.FilterConverterFactory;
import org.infinispan.server.functional.extensions.filters.RawStaticCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.RawStaticConverterFactory;
import org.infinispan.server.functional.extensions.filters.SimpleConverterFactory;
import org.infinispan.server.functional.extensions.filters.StaticCacheEventFilterFactory;
import org.infinispan.server.functional.extensions.filters.StaticConverterFactory;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Entities.CustomEvent.class,
            Entities.CustomKey.class,
            Entities.Person.class,
            DynamicCacheEventFilterFactory.DynamicCacheEventFilter.class,
            RawStaticCacheEventFilterFactory.RawStaticCacheEventFilter.class,
            StaticCacheEventFilterFactory.StaticCacheEventFilter.class,
            DynamicConverterFactory.DynamicConverter.class,
            FilterConverterFactory.FilterConverter.class,
            RawStaticConverterFactory.RawStaticConverter.class,
            SimpleConverterFactory.SimpleConverter.class,
            StaticConverterFactory.StaticConverter.class
      }
)
public interface Entities extends GeneratedSchema {
   Entities INSTANCE = new EntitiesImpl();

   /**
    * This class is annotated with the infinispan Protostream support annotations. With this method, you don't need to
    * define a protobuf file and a marshaller for the object.
    */
   final class Person {

      @ProtoField(number = 1)
      String firstName;

      @ProtoField(number = 2)
      String lastName;

      @ProtoField(number = 3, defaultValue = "-1")
      int bornYear;

      @ProtoField(number = 4)
      String bornIn;

      @ProtoFactory
      public Person(String firstName, String lastName, int bornYear, String bornIn) {
         this.firstName = firstName;
         this.lastName = lastName;
         this.bornYear = bornYear;
         this.bornIn = bornIn;
      }

      @Override
      public String toString() {
         return "Person{" +
               "firstName='" + firstName + '\'' +
               ", lastName='" + lastName + '\'' +
               ", bornYear='" + bornYear + '\'' +
               ", bornIn='" + bornIn + '\'' +
               '}';
      }
   }

   final class CustomKey {
      @ProtoField(number = 1, defaultValue = "0")
      final int id;

      @ProtoFactory
      public CustomKey(int id) {
         this.id = id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         CustomKey customKey = (CustomKey) o;
         return id == customKey.id;
      }

      @Override
      public int hashCode() {
         return id;
      }
   }

   final class CustomEvent<K> {
      @ProtoField(1)
      final WrappedMessage key;

      @ProtoField(2)
      final String value;

      @ProtoField(number = 3, defaultValue = "-1")
      final long timestamp;

      @ProtoField(number = 4, defaultValue = "0")
      final int counter;

      public CustomEvent(K key, String value, int counter) {
         this(new WrappedMessage(key), value, System.nanoTime(), counter);
      }

      @ProtoFactory
      CustomEvent(WrappedMessage key, String value, long timestamp, int counter) {
         this.key = key;
         this.value = value;
         this.timestamp = timestamp;
         this.counter = counter;
      }

      public WrappedMessage getKey() {
         return key;
      }

      public String getValue() {
         return value;
      }

      public long getTimestamp() {
         return timestamp;
      }

      public int getCounter() {
         return counter;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomEvent<?> that = (CustomEvent<?>) o;

         if (counter != that.counter) return false;
         if (!key.getValue().equals(that.key.getValue())) return false;
         return Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
         int result = key.getValue().hashCode();
         result = 31 * result + (value != null ? value.hashCode() : 0);
         result = 31 * result + counter;
         return result;
      }

      @Override
      public String toString() {
         return "CustomEvent{" +
               "key=" + key.getValue() +
               ", value='" + value + '\'' +
               ", timestamp=" + timestamp +
               ", counter=" + counter +
               '}';
      }
   }
}
