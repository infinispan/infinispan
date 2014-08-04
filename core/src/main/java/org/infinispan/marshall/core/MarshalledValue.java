package org.infinispan.marshall.core;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.io.ExpandableMarshalledValueByteStream;
import org.infinispan.io.ImmutableMarshalledValueByteStream;
import org.infinispan.io.MarshalledValueByteStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.ExtendedRiverUnmarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Set;

/**
 * Wrapper that wraps cached data, providing lazy deserialization using the calling thread's context class loader.
 * <p/>
 * The {@link org.infinispan.interceptors.MarshalledValueInterceptor} handles transparent wrapping/unwrapping of cached
 * data.
 * <p/>
 * <b>NOTE:</b> the <t>equals()</tt> method of this class will compare binary representations (byte arrays).
 * <p/>
 * Note also that this will affect the way keys stored in the cache will work, if <tt>storeAsBinary</tt> is used, since
 * comparisons happen on the key which will be wrapped by a {@link MarshalledValue}.  Implementers of <tt>equals()</tt>
 * methods of their keys need to be aware of this.
 * <p />
 *
 * This class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the externalizer method.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @see org.infinispan.interceptors.MarshalledValueInterceptor
 * @since 4.0
 */
public final class MarshalledValue implements Externalizable {

   private MarshalledValueByteStream raw;
   private int serialisedSize; //size of serialized representation
   private int cachedHashCode;
   // A marshaller is needed at construction time to handle equals/hashCode impls
   private transient StreamingMarshaller marshaller;

   public MarshalledValue() {
      // For JDK serialization
   }

   /**
    * Construct a Marshalledvalue from the already serialized bytes.  The hashCode provided should be
    * the hashCode of the object when it is deserialized.  Great <b>CARE</b> should be taken to guarantee
    * the hashCode is correct, or else the hashCode contract will be broken and things like
    * {@link java.util.Map#containsKey(Object)} will not work properly.
    * @param bytes The serialized form of the object
    * @param hashCode The hashCode of the object when it was deserialized
    * @param marshaller The marshaller to use to deserialize the object
    */
   public MarshalledValue(byte[] bytes, int hashCode, StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
      this.raw = new ImmutableMarshalledValueByteStream(bytes);
      this.cachedHashCode = hashCode;
      this.serialisedSize = bytes.length;
   }

   public MarshalledValue(Object instance, StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
      this.raw = serialize(instance);
      this.serialisedSize = raw.size();
      this.cachedHashCode = instance.hashCode();
   }

   /**
    * Should only be called from a synchronized method
    */
   private MarshalledValueByteStream serialize(Object instance) {
      try {
         // Do NOT set instance to null over here, since it may be used elsewhere (e.g., in a cache listener).
         // this will be compacted by the MarshalledValueInterceptor when the call returns.
         MarshalledValueByteStream baos = new ExpandableMarshalledValueByteStream(this.serialisedSize);
         ObjectOutput out = marshaller.startObjectOutput(baos, true, this.serialisedSize);
         try {
            marshaller.objectToObjectStream(instance, out);
         } finally {
            marshaller.finishObjectOutput(out);
         }
         return baos;
      } catch (Exception e) {
         throw new CacheException("Unable to marshall value " + instance, e);
      }
   }

   private Object deserialize() {
      try {
         // StreamingMarshaller underneath deals with making sure the right classloader is set.
         return marshaller.objectFromByteBuffer(raw.getRaw(), 0, raw.size());
      }
      catch (Exception e) {
         throw new CacheException("Unable to unmarshall value", e);
      }
   }

   public MarshalledValueByteStream getRaw() {
      return raw;
   }

   /**
    * Returns the 'cached' instance
    */
   public Object get() {
      return deserialize();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || MarshalledValue.class != o.getClass()) {
         return false;
      }

      MarshalledValue that = (MarshalledValue) o;
      MarshalledValueByteStream thisRaw = this.raw;
      MarshalledValueByteStream thatRaw = that.raw;
      if (thisRaw != null && thatRaw != null) return thisRaw.equals(thatRaw);

      return false;
   }

   @Override
   public int hashCode() {
      return cachedHashCode;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder()
         .append("MarshalledValue{")
         .append("serialized=").append(Util.printArray(raw.getRaw(), false))
         .append("}@").append(Util.hexIdHashCode(this));
      return sb.toString();
   }

   /**
    * Tests whether the type should be excluded from MarshalledValue wrapping.
    *
    * @param type type to test.  Should not be null.
    * @return true if it should be excluded from MarshalledValue wrapping.
    */
   public static boolean isTypeExcluded(Class<?> type) {
      return type.equals(String.class) || type.isPrimitive() ||
            type.equals(Void.class) || type.equals(Boolean.class) || type.equals(Character.class) ||
            type.equals(Byte.class) || type.equals(Short.class) || type.equals(Integer.class) ||
            type.equals(Long.class) || type.equals(Float.class) || type.equals(Double.class) ||
            (type.isArray() && isTypeExcluded(type.getComponentType())) || type.equals(GlobalTransaction.class) || Address.class.isAssignableFrom(type) ||
            ReplicableCommand.class.isAssignableFrom(type) || type.equals(MarshalledValue.class);
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(serialisedSize);
      out.write(raw.getRaw());
      out.writeInt(cachedHashCode);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      serialisedSize = in.readInt();
      byte[] bytes = new byte[serialisedSize];
      in.readFully(bytes);
      raw = new ImmutableMarshalledValueByteStream(bytes);
      cachedHashCode = in.readInt();
      // If the marshalled value is being serialized via the JDK, it's not in
      // an environment where the cache marshaller can be injected, so the
      // only alternative available is really the generic JBoss Marshaller,
      // used in potentially non-cache environments, i.e. hot rod client.
      marshaller = new GenericJBossMarshaller();
   }

   public static class Externalizer extends AbstractExternalizer<MarshalledValue> {
      private final StreamingMarshaller globalMarshaller;

      public Externalizer(StreamingMarshaller globalMarshaller) {
         this.globalMarshaller = globalMarshaller;
      }

      @Override
      public void writeObject(ObjectOutput output, MarshalledValue mv) throws IOException {
         int hashCode = mv.hashCode();
         MarshalledValueByteStream raw = mv.getRaw();
         int rawLength = raw.size();
         UnsignedNumeric.writeUnsignedInt(output, rawLength);
         output.write(raw.getRaw(), 0, rawLength);
         output.writeInt(hashCode);
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
               marshaller = globalMarshaller;
         } else {
            marshaller = globalMarshaller;
         }

         return new MarshalledValue(raw, hc, marshaller);
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
