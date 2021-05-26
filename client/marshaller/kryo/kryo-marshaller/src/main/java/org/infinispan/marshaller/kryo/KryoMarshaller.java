package org.infinispan.marshaller.kryo;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.LazyByteArrayOutputStream;
import org.infinispan.commons.marshall.AbstractMarshaller;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

/**
 * @author Ryan Emerson
 * @since 9.0
 * @deprecated since 12.0 without replacement, will be removed in 15.0 ISPN-12152
 */
@Deprecated
public class KryoMarshaller extends AbstractMarshaller {

   private static final List<SerializerRegistryService> serializerServices = new ArrayList<>();

   static {
      ServiceLoader.load(SerializerRegistryService.class, KryoMarshaller.class.getClassLoader())
            .forEach(serializerServices::add);
   }

   private final KryoPool pool;

   public KryoMarshaller() {
      KryoFactory factory = () -> {
         Kryo kryo = new Kryo();
         serializerServices.forEach(service -> service.register(kryo));
         return kryo;
      };
      this.pool = new KryoPool.Builder(factory).softReferences().build();
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int length) {
      return pool.run((kryo) -> {
         try (Input input = new Input(bytes, offset, length)) {
            return kryo.readClassAndObject(input);
         }
      });
   }

   @Override
   protected ByteBuffer objectToBuffer(Object obj, int estimatedSize) {
      return pool.run((kryo) -> {
         try (Output output = new Output(new LazyByteArrayOutputStream(estimatedSize), estimatedSize)) {
            kryo.writeClassAndObject(output, obj);
            return ByteBufferImpl.create(output.toBytes());
         }
      });
   }

   @Override
   public boolean isMarshallable(Object obj) {
      try {
         objectToBuffer(obj);
         return true;
      } catch (Throwable t) {
         return false;
      }
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_KRYO;
   }
}
