/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

import org.infinispan.api.CacheException;
import org.infinispan.api.io.ExposedByteArrayOutputStream;
import org.infinispan.api.marshall.StreamingMarshaller;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.jboss.ExtendedRiverUnmarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Wrapper that wraps cached data, providing lazy deserialization using the calling thread's context class loader.
 * <p/>
 * The {@link org.infinispan.interceptors.MarshalledValueInterceptor} handles transparent wrapping/unwrapping of cached
 * data.
 * <p/>
 * <b>NOTE:</b> the <t>equals()</tt> method of this class will either compare binary representations (byte arrays) or
 * delegate to the wrapped instance's <tt>equals()</tt> method, depending on whether both instances being compared are
 * in serialized or deserialized form.  If one of the instances being compared is in one form and the other in another
 * form, then one instance is either serialized or deserialized, the preference will be to compare object representations,
 * unless the instance is {@link #compact(boolean, boolean)}ed and a preference is set accordingly.
 * <p/>
 * Note also that this will affect the way keys stored in the cache will work, if <tt>storeAsBinary</tt> is used, since
 * comparisons happen on the key which will be wrapped by a {@link MarshalledValue}.  Implementers of <tt>equals()</tt>
 * methods of their keys need to be aware of this.
 * <p />
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @see org.infinispan.interceptors.MarshalledValueInterceptor
 * @since 4.0
 */
public class MarshalledValue {
   volatile protected Object instance;
   volatile protected byte[] raw;
   volatile protected int serialisedSize = 128; //size of serialized representation: initial value is a guess
   volatile private int cachedHashCode = 0;
   // by default equals() will test on the instance rather than the byte array if conversion is required.
   private transient volatile boolean equalityPreferenceForInstance = true;
   // A marshaller is needed at construction time to handle equals/hashCode impls
   private final StreamingMarshaller marshaller;

   public MarshalledValue(Object instance, boolean equalityPreferenceForInstance, StreamingMarshaller marshaller) {
      if (instance == null) throw new NullPointerException("Null values cannot be wrapped as MarshalledValues!");

      this.instance = instance;
      this.equalityPreferenceForInstance = equalityPreferenceForInstance;
      this.marshaller = marshaller;
   }

   private MarshalledValue(byte[] raw, int cachedHashCode, StreamingMarshaller marshaller) {
      init(raw, cachedHashCode);
      this.marshaller = marshaller;
   }

   private void init(byte[] raw, int cachedHashCode) {
      // for unmarshalling
      this.raw = raw;
      this.serialisedSize = raw.length;
      this.cachedHashCode = cachedHashCode;
   }

   public synchronized byte[] serialize() {
      return serialize0();
   }

   /**
    * Should only be called from a synchronized method
    */
   private byte[] serialize0() {
      byte[] rawValue = raw;
      if (rawValue == null) {
         try {
            // Do NOT set instance to null over here, since it may be used elsewhere (e.g., in a cache listener).
            // this will be compacted by the MarshalledValueInterceptor when the call returns.
            ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(this.serialisedSize);
            ObjectOutput out = marshaller.startObjectOutput(baos, true);
            try {
               marshaller.objectToObjectStream(instance, out);
            } finally {
               marshaller.finishObjectOutput(out);
            }
            final byte[] buf = baos.getRawBuffer();
            final int length = baos.size();
            if (buf.length == length) {
               // in this case we can avoid duplicating the buffer
               rawValue = buf;
            }
            else {
               serialisedSize = length;
               rawValue = new byte[length];
               System.arraycopy(buf, 0, rawValue, 0, length);
            }
            raw = rawValue;
         } catch (Exception e) {
            throw new CacheException("Unable to marshall value " + instance, e);
         }
      }
      return rawValue;
   }

   public synchronized Object deserialize() {
      return deserialize0();
   }

   /**
    * Should only be called from a synchronized method
    */
   private Object deserialize0() {
      Object instanceValue = instance;
      if (instanceValue == null) {
         try {
            // StreamingMarshaller underneath deals with making sure the right classloader is set.
            instanceValue = marshaller.objectFromByteBuffer(raw);
            instance = instanceValue;
            return instanceValue;
         }
         catch (Exception e) {
            throw new CacheException("Unable to unmarshall value", e);
         }
      }
      return instanceValue;
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
   public synchronized void compact(boolean preferSerializedRepresentation, boolean force) {
      // reset the equalityPreference
      equalityPreferenceForInstance = true;
      Object thisInstance = this.instance;
      byte[] thisRaw = this.raw;
      if (force) {
         if (preferSerializedRepresentation && thisRaw == null) {
            // Accessing a synchronized method from an already synchronized
            // method is expensive, so delegate to a private not synched method.
            thisRaw = serialize0();
         }
         else if (!preferSerializedRepresentation && thisInstance == null){
            // Accessing a synchronized method from an already synchronized
            // method is expensive, so delegate to a private not synched method.
            thisInstance = deserialize0();
         }
      }

      if (thisInstance != null && thisRaw != null) {
         // need to loose one representation!
         if (preferSerializedRepresentation) {
            //in both branches we first set one then null the other, so that there's always one available
            //to read from those methods not being synchronized
            raw = thisRaw;
            instance = null;
         } else {
            instance = thisInstance;
            raw = null;
         }
      }
   }

   public byte[] getRaw() {
      byte[] rawValue = raw;
      if (rawValue == null){
         rawValue = serialize();
      }
      return rawValue;
   }

   /**
    * Returns the 'cached' instance
    */
   public Object get() {
      Object value = instance;
      if (value == null) {
         value = deserialize();
      }
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MarshalledValue that = (MarshalledValue) o;
      final boolean preferInstanceEquality = equalityPreferenceForInstance && that.equalityPreferenceForInstance;

      // if both versions are serialized or deserialized, just compare the relevant representations,
      // but attempt the operations in order to respect the value of equalityPreferenceForInstance
      Object thisInstance = this.instance;
      Object thatInstance = that.instance;
      //test the default equality first so we might skip some work:
      if (preferInstanceEquality && thisInstance != null && thatInstance != null) return thisInstance.equals(thatInstance);
      
      byte[] thisRaw = this.raw;
      byte[] thatRaw = that.raw;
      if (thisRaw != null && thatRaw != null) return Arrays.equals(thisRaw, thatRaw);
      if (thisInstance != null && thatInstance != null) return thisInstance.equals(thatInstance);

      // if conversion of one representation to the other is necessary, then see which we prefer converting.
      if (preferInstanceEquality) {
         if (thisInstance == null) {
            thisInstance = this.deserialize();
         }
         if (thatInstance == null) {
            thatInstance = that.deserialize();
         }
         return thisInstance.equals(thatInstance);
      } else {
         if (thisRaw == null) {
            thisRaw = this.serialize();
         }
         if (thatRaw == null) {
            thatRaw = that.serialize();
         }
         return Arrays.equals(thisRaw, thatRaw);
      }
   }

   @Override
   public int hashCode() {
      //make a local copy to avoid multiple read/writes on the volatile field
      int value = cachedHashCode;
      if (value == 0) {
         Object localInstance = deserialize();
         value = localInstance.hashCode();
         if (value == 0) // degenerate case
         {
            value = 0xFEED;
         }
         cachedHashCode = value;
      }
      return value;
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

   public static class Externalizer extends AbstractExternalizer<MarshalledValue> {
      private GlobalComponentRegistry gcr;

      public void inject(GlobalComponentRegistry gcr) {
         this.gcr = gcr;
      }

      @Override
      public void writeObject(ObjectOutput output, MarshalledValue mv) throws IOException {
         byte[] raw = mv.getRaw();
         UnsignedNumeric.writeUnsignedInt(output, raw.length);
         output.write(raw);
         output.writeInt(mv.hashCode());
      }

      @Override
      public MarshalledValue readObject(ObjectInput input) throws IOException {
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] raw = new byte[length];
         input.readFully(raw);
         int hc = input.readInt();

         // A better way of sending down context information is needed in the future
         StreamingMarshaller marshaller;
         if (input instanceof ExtendedRiverUnmarshaller) {
            StreamingMarshaller ispnMarshaller =
                  ((ExtendedRiverUnmarshaller) input).getInfinispanMarshaller();
            if (ispnMarshaller != null)
               marshaller = ispnMarshaller;
            else
               marshaller = getGlobalMarshaller();
         } else {
            marshaller = getGlobalMarshaller();
         }

         return new MarshalledValue(raw, hc, marshaller);
      }

      private StreamingMarshaller getGlobalMarshaller() {
         return gcr.getComponent(
               StreamingMarshaller.class, KnownComponentNames.GLOBAL_MARSHALLER);
      }

      @Override
      public Integer getId() {
         return Ids.MARSHALLED_VALUE;
      }

      @Override
      public Set<Class<? extends MarshalledValue>> getTypeClasses() {
         return Util.<Class<? extends MarshalledValue>>asSet(MarshalledValue.class);
      }

   }
}
