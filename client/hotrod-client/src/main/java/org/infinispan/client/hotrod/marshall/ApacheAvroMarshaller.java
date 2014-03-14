package org.infinispan.client.hotrod.marshall;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.AbstractMarshaller;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a portable serialization marshaller based on Apache Avro. It supports basic type and collection marshalling.
 * Basic types include UTF-8 String, int long, float, double, boolean and null, and the collections supported include
 * arrays, list, map and set composed of basic types.
 *
 * Primitive types short and byte are not supported per se. Instead, pass integers which will be encoded efficiently
 * using variable-length (http://lucene.apache.org/java/2_4_0/fileformats.html#VInt) zig zag
 * (http://code.google.com/apis/protocolbuffers/docs/encoding.html#types) coding.
 *
 * Primitive arrays not supported except byte arrays. Instead, use their object counter partners, i.e. Integer...etc.
 *
 * For more detailed information, go to: http://community.jboss.org/docs/DOC-15774
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class ApacheAvroMarshaller extends AbstractMarshaller {

   private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
   private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
   private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
   private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
   private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
   private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
   private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
   private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
   private static final Schema STRING_ARRAY_SCHEMA = Schema.createArray(STRING_SCHEMA);
   private static final Schema INT_ARRAY_SCHEMA = Schema.createArray(INT_SCHEMA);
   private static final Schema LONG_ARRAY_SCHEMA = Schema.createArray(LONG_SCHEMA);
   private static final Schema FLOAT_ARRAY_SCHEMA = Schema.createArray(FLOAT_SCHEMA);
   private static final Schema DOUBLE_ARRAY_SCHEMA = Schema.createArray(DOUBLE_SCHEMA);
   private static final Schema BOOLEAN_ARRAY_SCHEMA = Schema.createArray(BOOLEAN_SCHEMA);

   private static final MarshallableType STRING_TYPE = new StringMarshallableType(0);
   private static final MarshallableType INT_TYPE = new MarshallableType(INT_SCHEMA, 1);
   private static final MarshallableType LONG_TYPE = new MarshallableType(LONG_SCHEMA, 2);
   private static final MarshallableType FLOAT_TYPE = new MarshallableType(FLOAT_SCHEMA, 3);
   private static final MarshallableType DOUBLE_TYPE = new MarshallableType(DOUBLE_SCHEMA, 4);
   private static final MarshallableType BOOLEAN_TYPE = new MarshallableType(BOOLEAN_SCHEMA, 5);
   private static final MarshallableType BYTES_TYPE = new BytesMarshallableType(6);
   private static final MarshallableType NULL_TYPE = new MarshallableType(NULL_SCHEMA, 7);
   private static final MarshallableType STRING_ARRAY_TYPE = new StringArrayMarshallableType(8);
   private static final MarshallableType INT_ARRAY_TYPE = new ArrayMarshallableType<Integer>(INT_ARRAY_SCHEMA, Integer.class, 9);
   private static final MarshallableType LONG_ARRAY_TYPE = new ArrayMarshallableType<Long>(LONG_ARRAY_SCHEMA, Long.class, 10);
   private static final MarshallableType DOUBLE_ARRAY_TYPE = new ArrayMarshallableType<Double>(DOUBLE_ARRAY_SCHEMA, Double.class, 11);
   private static final MarshallableType FLOAT_ARRAY_TYPE = new ArrayMarshallableType<Float>(FLOAT_ARRAY_SCHEMA, Float.class, 12);
   private static final MarshallableType BOOLEAN_ARRAY_TYPE = new ArrayMarshallableType<Boolean>(BOOLEAN_ARRAY_SCHEMA, Boolean.class, 13);
   private static final EncoderFactory AVRO_ENCODER_FACTORY = EncoderFactory.get();
   private static final DecoderFactory AVRO_DECODER_FACTORY = DecoderFactory.get();

   private final MarshallableType listType = new ListMarshallableType(14, this);
   private final MarshallableType mapType = new MapMarshallableType(15, this);
   private final MarshallableType setType = new SetMarshallableType(16, this);

   private MarshallableType getType(int type) {
      switch (type) {
         case 0: return STRING_TYPE;
         case 1: return INT_TYPE;
         case 2: return LONG_TYPE;
         case 3: return FLOAT_TYPE;
         case 4: return DOUBLE_TYPE;
         case 5: return BOOLEAN_TYPE;
         case 6: return BYTES_TYPE;
         case 7: return NULL_TYPE;
         case 8: return STRING_ARRAY_TYPE;
         case 9: return INT_ARRAY_TYPE;
         case 10: return LONG_ARRAY_TYPE;
         case 11: return DOUBLE_ARRAY_TYPE;
         case 12: return FLOAT_ARRAY_TYPE;
         case 13: return BOOLEAN_ARRAY_TYPE;
         case 14: return listType;
         case 15: return mapType;
         case 16: return setType;
         default: throw new CacheException("Unknown type " + type);
      }
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      Encoder encoder = AVRO_ENCODER_FACTORY.directBinaryEncoder(baos, null);
      objectToBuffer(o, encoder);
      return new ByteBufferImpl(baos.getRawBuffer(), 0, baos.size());
   }

   private void objectToBuffer(Object o, Encoder encoder) throws IOException {
      if (o == null) {
         NULL_TYPE.write(o, encoder);
      } else {
         Class<?> clazz = o.getClass();
         MarshallableType type;
         if (clazz.equals(String.class)) type = STRING_TYPE;
         else if (clazz.equals(byte[].class)) type = BYTES_TYPE;
         else if (clazz.equals(Boolean.class)) type = BOOLEAN_TYPE;
         else if (clazz.equals(Integer.class)) type = INT_TYPE;
         else if (clazz.equals(Long.class)) type = LONG_TYPE;
         else if (clazz.equals(Float.class)) type = FLOAT_TYPE;
         else if (clazz.equals(Double.class)) type = DOUBLE_TYPE;
         else if (clazz.equals(String[].class)) type = STRING_ARRAY_TYPE;
         else if (clazz.equals(Integer[].class)) type = INT_ARRAY_TYPE;
         else if (clazz.equals(Long[].class)) type = LONG_ARRAY_TYPE;
         else if (clazz.equals(Float[].class)) type = FLOAT_ARRAY_TYPE;
         else if (clazz.equals(Double[].class)) type = DOUBLE_ARRAY_TYPE;
         else if (clazz.equals(Boolean[].class)) type = BOOLEAN_ARRAY_TYPE;
         else if (o instanceof List) type = listType;
         else if (o instanceof Map) type = mapType;
         else if (o instanceof Set) type = setType;
         else
            throw new CacheException("Unsupported type: " + clazz);

         type.write(o, encoder);
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      Decoder decoder = AVRO_DECODER_FACTORY.binaryDecoder(buf, offset, length, null);
      return objectFromByteBuffer(decoder);
   }

   private Object objectFromByteBuffer(Decoder decoder) throws IOException {
      int type = decoder.readInt();
      return getType(type).read(decoder);
   }

   @Override
   public boolean isMarshallable(Object o) {
      Class<?> clazz = o.getClass();
      return clazz.equals(String.class) || clazz.equals(byte[].class)
            || clazz.equals(Boolean.class) || clazz.equals(Integer.class)
            || clazz.equals(Long.class) || clazz.equals(Float.class)
            || clazz.equals(Double.class) || clazz.equals(String[].class)
            || clazz.equals(Integer[].class) || clazz.equals(Long[].class)
            || clazz.equals(Float[].class) || clazz.equals(Double[].class)
            || clazz.equals(Boolean[].class) || o instanceof List
            || o instanceof Map || o instanceof Set;
   }

   private static class MarshallableType {
      final Schema schema;
      final int id;

      MarshallableType(Schema schema, int id) {
         this.schema = schema;
         this.id = id;
      }

      Object read(Decoder decoder) throws IOException {
         return new GenericDatumReader(schema).read(null, decoder);
      }

      void write(Object o, Encoder encoder) throws IOException {
         GenericDatumWriter<Object> writer = new GenericDatumWriter(schema); // TODO: Could this be cached? Maybe, but ctor is very cheap
         encoder.writeInt(id);
         write(writer, o, encoder);
      }

      void write(GenericDatumWriter<Object> writer, Object o, Encoder encoder) throws IOException {
         writer.write(o, encoder);
      }

   }

   private static class StringMarshallableType extends MarshallableType {
      StringMarshallableType(int id) {
         super(STRING_SCHEMA, id);
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         return new GenericDatumReader(schema).read(null, decoder).toString();
      }

      @Override
      void write(GenericDatumWriter<Object> writer, Object o, Encoder encoder) throws IOException {
         writer.write(new Utf8((String) o), encoder);
      }
   }

   private static class BytesMarshallableType extends MarshallableType {
      BytesMarshallableType(int id) {
         super(BYTES_SCHEMA, id);
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         java.nio.ByteBuffer byteBuffer = (java.nio.ByteBuffer) new GenericDatumReader(schema).read(null, decoder);
         byte[] bytes = new byte[byteBuffer.limit()]; // TODO: Limit or capacity ? Limit works
         byteBuffer.get(bytes);
         return bytes;
      }

      @Override
      void write(GenericDatumWriter<Object> writer, Object o, Encoder encoder) throws IOException {
         writer.write(java.nio.ByteBuffer.wrap((byte[]) o), encoder);
      }
   }

   private static class StringArrayMarshallableType extends MarshallableType {
      StringArrayMarshallableType(int id) {
         super(STRING_ARRAY_SCHEMA, id);
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         GenericData.Array<Utf8> utf8s = (GenericData.Array<Utf8>) new GenericDatumReader(schema).read(null, decoder);
         List<String> strings = new ArrayList<String>((int) utf8s.size());
         for (Utf8 utf8 : utf8s)
            strings.add(utf8.toString());
         return strings.toArray(new String[0]);
      }

      @Override
      void write(GenericDatumWriter<Object> writer, Object o, Encoder encoder) throws IOException {
         String[] strings = (String[]) o;
         GenericData.Array<Utf8> array = new GenericData.Array(strings.length, schema);
         for (String str : strings)
            array.add(new Utf8(str));
         writer.write(array, encoder);
      }
   }

   private static class ArrayMarshallableType<T> extends MarshallableType {
      private final Class<T> type;

      ArrayMarshallableType(Schema schema, Class<T> type, int id) {
         super(schema, id);
         this.type = type;
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         GenericData.Array<T> avroArray = (GenericData.Array<T>) new GenericDatumReader(schema).read(null, decoder);
         List<T> list = new ArrayList<T>((int) avroArray.size());
         for (T t : avroArray)
            list.add(t);
         T[] array = (T[]) Array.newInstance(type, list.size());
         return toArray(list, array);
      }

      private <T> T[] toArray(List<T> list, T... ts) {
         // Varargs hack!
         return list.toArray(ts);
      }

      @Override
      void write(GenericDatumWriter<Object> writer, Object o, Encoder encoder) throws IOException {
         T[] array = (T[]) o;
         GenericData.Array<T> avroArray = new GenericData.Array(array.length, schema);
         for (T t : array)
             avroArray.add(t);
         writer.write(avroArray, encoder);
      }
   }

   private static abstract class CollectionMarshallableType extends MarshallableType {
      final ApacheAvroMarshaller marshaller;

      CollectionMarshallableType(int id, ApacheAvroMarshaller marshaller) {
         super(null, id);
         this.marshaller = marshaller;
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         long size = decoder.readArrayStart();
         Collection<Object> collection = createCollection((int) size);
         for (int k = 0; k < size; k++)
            collection.add(marshaller.objectFromByteBuffer(decoder));
         return collection;
      }

      @Override
      void write(Object o, Encoder encoder) throws IOException {
         Collection<Object> collection = (Collection<Object>) o;
         encoder.writeInt(id);
         encoder.setItemCount(collection.size());
         for (Object element : collection)
            marshaller.objectToBuffer(element, encoder);
      }

      abstract Collection<Object> createCollection(int size);

   }

   private static class ListMarshallableType extends CollectionMarshallableType {
      ListMarshallableType(int id, ApacheAvroMarshaller marshaller) {
         super(id, marshaller);
      }

      @Override
      Collection<Object> createCollection(int size) {
         return new ArrayList<Object>(size);
      }
   }

   private static class MapMarshallableType extends CollectionMarshallableType {
      MapMarshallableType(int id, ApacheAvroMarshaller marshaller) {
         super(id, marshaller);
      }

      @Override
      Object read(Decoder decoder) throws IOException {
         long size = decoder.readArrayStart();
         Map<Object, Object> map = new HashMap<Object, Object>((int) size);
         for (int i = 0; i < size; i++)
            map.put(marshaller.objectFromByteBuffer(decoder), marshaller.objectFromByteBuffer(decoder));
         return map;
      }

      @Override
      void write(Object o, Encoder encoder) throws IOException {
         Map<Object, Object> map = (Map<Object, Object>) o;
         encoder.writeInt(id);
         encoder.setItemCount(map.size());
         for (Map.Entry<Object, Object> entry : map.entrySet()) {
            marshaller.objectToBuffer(entry.getKey(), encoder);
            marshaller.objectToBuffer(entry.getValue(), encoder);
         }

      }

      @Override
      Collection<Object> createCollection(int size) {
         return null; // Ignored for this class
      }
   }

   private class SetMarshallableType extends CollectionMarshallableType {
      public SetMarshallableType(int id, ApacheAvroMarshaller marshaller) {
         super(id, marshaller);
      }

      @Override
      Collection<Object> createCollection(int size) {
         return new HashSet<Object>(size);
      }
   }
}
