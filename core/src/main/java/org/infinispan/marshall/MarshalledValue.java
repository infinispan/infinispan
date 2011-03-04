/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Wrapper that wraps cached data, providing lazy deserialization using the calling thread's context class loader.
 * <p/>
 * The {@link org.infinispan.interceptors.MarshalledValueInterceptor} handles transparent wrapping/unwrapping of cached
 * data.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see org.infinispan.interceptors.MarshalledValueInterceptor
 * @since 4.0
 */
@Marshallable(externalizer = MarshalledValue.Externalizer.class, id = Ids.MARSHALLED_VALUE)
public class MarshalledValue {
   volatile protected Object instance;
   volatile protected byte[] raw;
   volatile private int cachedHashCode = 0;
   // by default equals() will test on the instance rather than the byte array if conversion is required.
   private transient volatile boolean equalityPreferenceForInstance = true;
   private final StreamingMarshaller marshaller;

   public MarshalledValue(Object instance, boolean equalityPreferenceForInstance, StreamingMarshaller marshaller) throws NotSerializableException {
      if (instance == null) throw new NullPointerException("Null values cannot be wrapped as MarshalledValues!");

      this.instance = instance;
      this.equalityPreferenceForInstance = equalityPreferenceForInstance;
      this.marshaller = marshaller;
   }

   public MarshalledValue(byte[] raw, int cachedHashCode, StreamingMarshaller marshaller) {
      init(raw, cachedHashCode);
      this.marshaller = marshaller;
   }

   public void init(byte[] raw, int cachedHashCode) {
      // for unmarshalling
      this.raw = raw;
      this.cachedHashCode = cachedHashCode;
   }

   public synchronized void serialize() {
      if (raw == null) {
         try {
            // Do NOT set instance to null over here, since it may be used elsewhere (e.g., in a cache listener).
            // this will be compacted by the MarshalledValueInterceptor when the call returns.
            ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(128);
            ObjectOutput out = marshaller.startObjectOutput(baos, true);
            try {
               marshaller.objectToObjectStream(instance, out);
            } finally {
               marshaller.finishObjectOutput(out);
            }
            byte[] buf = baos.getRawBuffer();
            int length = baos.size();
            raw = new byte[length];
            System.arraycopy(buf, 0, raw, 0, length);
         } catch (Exception e) {
            throw new CacheException("Unable to marshall value " + instance, e);
         } finally {
            
         }
      }
   }

   public synchronized void deserialize() {
      if (instance == null) {
         try {
            // StreamingMarshaller underneath deals with making sure the right classloader is set.
            instance = marshaller.objectFromByteBuffer(raw);
         }
         catch (Exception e) {
            throw new CacheException("Unable to unmarshall value", e);
         }
      }
   }

   /**
    * Compacts the references held by this class to a single reference.  If only one representation exists this method
    * is a no-op unless the 'force' parameter is used, in which case the reference held is forcefully switched to the
    * 'preferred representation'.
    * <p/>
    * Either way, a call to compact() will ensure that only one representation is held.
    * <p/>
    *
    * @param preferSerializedRepresentation if true and both representations exist, the serialized representation is
    *                                       favoured.  If false, the deserialized representation is preferred.
    * @param force                          ensures the preferred representation is maintained and the other released,
    *                                       even if this means serializing or deserializing.
    */
   public void compact(boolean preferSerializedRepresentation, boolean force) {
      // reset the equalityPreference
      equalityPreferenceForInstance = true;
      if (force) {
         if (preferSerializedRepresentation && raw == null) serialize();
         else if (!preferSerializedRepresentation && instance == null) deserialize();
      }

      if (instance != null && raw != null) {
         // need to lose one representation!

         if (preferSerializedRepresentation) {
            nullifyInstance();
         } else {
            raw = null;
         }
      }
   }

   private synchronized void nullifyInstance() {
      instance = null;
   }

   public byte[] getRaw() {
      if (raw == null) serialize();
      return raw;
   }

   /**
    * Returns the 'cached' instance. Impl note: this method is synchronized so that it synchronizez with the code that
    * nullifies the instance.
    *
    * @see #nullifyInstance()
    */
   public synchronized Object get() {
      if (instance == null) deserialize();
      return instance;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MarshalledValue that = (MarshalledValue) o;

      // if both versions are serialized or deserialized, just compare the relevant representations.
      if (raw != null && that.raw != null) return Arrays.equals(raw, that.raw);
      if (instance != null && that.instance != null) return instance.equals(that.instance);

      // if conversion of one representation to the other is necessary, then see which we prefer converting.
      if (equalityPreferenceForInstance && that.equalityPreferenceForInstance) {
         if (instance == null) deserialize();
         if (that.instance == null) that.deserialize();
         return instance.equals(that.instance);
      } else {
         if (raw == null) serialize();
         if (that.raw == null) that.serialize();
         return Arrays.equals(raw, that.raw);
      }
   }

   @Override
   public int hashCode() {
      if (cachedHashCode == 0) {
         // always calculate the hashcode based on the instance since this is where we're getting the equals()
         if (instance == null) deserialize();
         cachedHashCode = instance.hashCode();
         if (cachedHashCode == 0) // degenerate case
         {
            cachedHashCode = 0xFEED;
         }
      }
      return cachedHashCode;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder()
         .append("MarshalledValue{")
         .append("instance=").append(instance != null ? instance.toString() : "<serialized>")
         .append(", serialized=").append(raw != null ?  Util.printArray(raw, false) : "false")
         .append(", cachedHashCode=").append(cachedHashCode)
         .append("}@").append(Util.hexIdHashCode(this));
      return sb.toString();
   }

   public MarshalledValue setEqualityPreferenceForInstance(boolean equalityPreferenceForInstance) {
      this.equalityPreferenceForInstance = equalityPreferenceForInstance;
      return this;
   }

   /**
    * Tests whether the type should be excluded from MarshalledValue wrapping.
    *
    * @param type type to test.  Should not be null.
    * @return true if it should be excluded from MarshalledValue wrapping.
    */
   public static boolean isTypeExcluded(Class type) {
      return type.equals(String.class) || type.isPrimitive() ||
            type.equals(Void.class) || type.equals(Boolean.class) || type.equals(Character.class) ||
            type.equals(Byte.class) || type.equals(Short.class) || type.equals(Integer.class) ||
            type.equals(Long.class) || type.equals(Float.class) || type.equals(Double.class) ||
            (type.isArray() && isTypeExcluded(type.getComponentType())) || type.equals(GlobalTransaction.class) || Address.class.isAssignableFrom(type) ||
            ReplicableCommand.class.isAssignableFrom(type) || type.equals(MarshalledValue.class);
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      private StreamingMarshaller marshaller;
      
      public void inject(StreamingMarshaller marshaller) {
         this.marshaller = marshaller;
      }
      
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         MarshalledValue mv = ((MarshalledValue) subject);
         byte[] raw = mv.getRaw();
         UnsignedNumeric.writeUnsignedInt(output, raw.length);
         output.write(raw);
         output.writeInt(mv.hashCode());      
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] raw = new byte[length];
         input.readFully(raw);
         int hc = input.readInt();
         return new MarshalledValue(raw, hc, marshaller);
      }
   }
}
