package org.infinispan.commons.marshall.proto;

import java.io.IOException;
import java.util.Objects;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A wrapper class that allows a protostream marshallable {@link Object} to be added to a annotated Pojo without knowing
 * the implementation class at compile time.
 *
 * TODO do we want to support this for users? Or should they always know the type?
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class RuntimeMarshallableWrapper {

   @ProtoField(number = 1)
   volatile byte[] bytes;

   volatile Object object;

   RuntimeMarshallableWrapper() {
   }

   public RuntimeMarshallableWrapper(Object object) {
      Objects.requireNonNull(object);
      this.object = object;
   }

   public Object get() {
      if (object == null)
         throw new IllegalStateException("Wrapped object has not been unmarshalled");
      return object;
   }

   void marshall(Marshaller marshaller) throws IOException, InterruptedException {
      if (bytes == null) {
         assert object != null;
         this.bytes = marshaller.objectToByteBuffer(object);
      }
   }

   void unmarshall(Marshaller marshaller) throws ClassNotFoundException, IOException {
      if (object == null) {
         assert bytes != null && bytes.length > 0;
         object = marshaller.objectFromByteBuffer(bytes);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RuntimeMarshallableWrapper that = (RuntimeMarshallableWrapper) o;
      return Objects.equals(object, that.object);
   }

   @Override
   public int hashCode() {
      return Objects.hash(object);
   }

   @Override
   public String toString() {
      return "RuntimeMarshallableWrapper{" +
            "object=" + object +
            '}';
   }
}
