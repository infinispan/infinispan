package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;

/**
 * Wraps encoding classes and wrapping classes in a single object.
 * <p>
 * This class is final to prevent issues as it is usually not marshalled
 * as polymorphic object but directly using {@link #writeTo(ObjectOutput, EncodingClasses)}
 * and {@link #readFrom(ObjectInput)}.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class EncodingClasses {

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   public EncodingClasses(Class<? extends Encoder> keyEncoderClass,
                          Class<? extends Encoder> valueEncoderClass,
                          Class<? extends Wrapper> keyWrapperClass,
                          Class<? extends Wrapper> valueWrapperClass) {
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   public Class<? extends Encoder> getKeyEncoderClass() {
      return keyEncoderClass;
   }

   public Class<? extends Encoder> getValueEncoderClass() {
      return valueEncoderClass;
   }

   public Class<? extends Wrapper> getKeyWrapperClass() {
      return keyWrapperClass;
   }

   public Class<? extends Wrapper> getValueWrapperClass() {
      return valueWrapperClass;
   }

   public static EncodingClasses readFrom(ObjectInput input) throws ClassNotFoundException, IOException {
      Class<? extends Encoder> encoderKey = (Class<? extends Encoder>) input.readObject();
      Class<? extends Encoder> encoderValue = (Class<? extends Encoder>) input.readObject();
      Class<? extends Wrapper> wrapperKey = (Class<? extends Wrapper>) input.readObject();
      Class<? extends Wrapper> wrapperValue = (Class<? extends Wrapper>) input.readObject();
      EncodingClasses encodingClasses = null;
      if(encoderKey != null || encoderValue != null || wrapperKey != null || wrapperValue != null){
         encodingClasses = new EncodingClasses(encoderKey, encoderValue, wrapperKey, wrapperValue);
      }
      return encodingClasses;
   }

   public static void writeTo(ObjectOutput output, EncodingClasses encodingClasses) throws IOException {
      if(encodingClasses == null) {
         output.writeObject(null);
         output.writeObject(null);
         output.writeObject(null);
         output.writeObject(null);
      } else {
         output.writeObject(encodingClasses.keyEncoderClass);
         output.writeObject(encodingClasses.valueEncoderClass);
         output.writeObject(encodingClasses.keyWrapperClass);
         output.writeObject(encodingClasses.valueWrapperClass);
      }

   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EncodingClasses that = (EncodingClasses) o;

      return Objects.equals(keyEncoderClass, that.keyEncoderClass) &&
            Objects.equals(valueEncoderClass, that.valueEncoderClass) &&
            Objects.equals(keyWrapperClass, that.keyWrapperClass) &&
            Objects.equals(valueWrapperClass, that.valueWrapperClass);
   }

   @Override
   public int hashCode() {
      return Objects.hash(keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
   }

   @Override
   public String toString() {
      return "EncodingClasses{" +
            "keyEncoderClass=" + keyEncoderClass +
            ", valueEncoderClass=" + valueEncoderClass +
            ", keyWrapperClass=" + keyWrapperClass +
            ", valueWrapperClass=" + valueWrapperClass +
            '}';
   }
}
