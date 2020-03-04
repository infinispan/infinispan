package org.infinispan.eviction.impl.protostream;

import static org.infinispan.protostream.FileDescriptorSource.fromString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Adds support for primitive and primitive wrapper arrays.
 */
public class PrimitiveArrayCtx implements SerializationContextInitializer {

   static class PrimitiveArrayMarshaller implements MessageMarshaller<Object> {
      private static final String FIELD_NAME = "element";
      private final Class<?> primitiveType;

      public PrimitiveArrayMarshaller(Class<?> primitiveType) {
         this.primitiveType = primitiveType;
      }

      @Override
      public Object readFrom(ProtoStreamReader reader) throws IOException {
         switch (primitiveType.getSimpleName()) {
            case "Byte":
               Integer[] byteIntegers = reader.readArray(FIELD_NAME, Integer.class);
               Byte[] bytes = new Byte[byteIntegers.length];
               for (int i = 0; i < byteIntegers.length; i++) {
                  bytes[i] = byteIntegers[i].byteValue();
               }
               return bytes;
            case "Short":
               Integer[] shortIntegers = reader.readArray(FIELD_NAME, Integer.class);
               Short[] shorts = new Short[shortIntegers.length];
               for (int i = 0; i < shortIntegers.length; i++) {
                  shorts[i] = shortIntegers[i].shortValue();
               }
               return shorts;
            case "Integer":
               return reader.readArray(FIELD_NAME, Integer.class);
            case "Long":
               List<Long> longs = new ArrayList<>();
               reader.readCollection(FIELD_NAME, longs, Long.class);
               return longs;
            case "Double":
               return reader.readArray(FIELD_NAME, Double.class);
            case "String":
               return reader.readArray(FIELD_NAME, String.class);
            case "byte":
               return reader.readBytes(FIELD_NAME);
            case "short":
               int[] ints = reader.readInts(FIELD_NAME);
               short[] array = new short[ints.length];
               for (int i = 0; i < ints.length; i++) {
                  array[i] = (short) ints[i];
               }
               return array;
            case "int":
               return reader.readInts(FIELD_NAME);
            case "long":
               return reader.readLongs(FIELD_NAME);
            case "double":
               return reader.readDoubles(FIELD_NAME);

            default:
               throw new IllegalArgumentException("Array type " + primitiveType.getSimpleName() + " not supported");
         }
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, Object o) throws IOException {
         if (o instanceof Byte[]) {
            Byte[] bytes = (Byte[]) o;
            Integer[] integers = new Integer[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
               integers[i] = bytes[i].intValue();
            }
            writer.writeArray(FIELD_NAME, integers, Integer.class);
            return;
         }
         if (o instanceof Short[]) {
            Short[] shorts = (Short[]) o;
            Integer[] integers = new Integer[shorts.length];
            for (int i = 0; i < shorts.length; i++) {
               integers[i] = shorts[i].intValue();
            }
            writer.writeArray(FIELD_NAME, integers, Integer.class);
            return;
         }
         if (o instanceof Integer[]) {
            writer.writeArray(FIELD_NAME, (Integer[]) o, Integer.class);
            return;
         }
         if (o instanceof Long[]) {
            writer.writeArray(FIELD_NAME, (Long[]) o, Long.class);
            return;
         }
         if (o instanceof Double[]) {
            writer.writeArray(FIELD_NAME, (Double[]) o, Double.class);
            return;
         }
         if (o instanceof String[]) {
            writer.writeArray(FIELD_NAME, (String[]) o, String.class);
            return;
         }
         if (o instanceof byte[]) {
            writer.writeBytes(FIELD_NAME, (byte[]) o);
            return;
         }
         if (o instanceof short[]) {
            short[] s = (short[]) o;
            int[] widened = new int[s.length];
            for (int i = 0; i < s.length; i++) {
               widened[i] = s[i];
            }
            writer.writeInts(FIELD_NAME, widened);
            return;
         }
         if (o instanceof int[]) {
            writer.writeInts(FIELD_NAME, (int[]) o);
            return;
         }
         if (o instanceof long[]) {
            writer.writeLongs(FIELD_NAME, (long[]) o);
            return;
         }
         if (o instanceof float[]) {
            writer.writeFloats(FIELD_NAME, (float[]) o);
            return;
         }
         if (o instanceof double[]) {
            writer.writeDoubles(FIELD_NAME, (double[]) o);
            return;
         }
         throw new IllegalArgumentException("Array type " + o.getClass() + " not supported");
      }

      @Override
      public String getTypeName() {
         return primitiveType.getSimpleName();
      }

      @Override
      public Class<?> getJavaClass() {
         switch (primitiveType.getSimpleName()) {
            case "byte":
               return byte[].class;
            case "short":
               return short[].class;
            case "int":
               return int[].class;
            case "long":
               return long[].class;
            case "float":
               return float[].class;
            case "double":
               return double[].class;
            case "Byte":
               return Byte[].class;
            case "Short":
               return Short[].class;
            case "Integer":
               return Integer[].class;
            case "Long":
               return Long[].class;
            case "Float":
               return Float[].class;
            case "Double":
               return Double[].class;
            case "String":
               return String[].class;
            default:
               throw new IllegalArgumentException("Type " + primitiveType + " not supported");
         }
      }
   }

   @Override
   public String getProtoFileName() {
      return "array-primitives.proto";
   }

   @Override
   public String getProtoFile() throws UncheckedIOException {
      return "message byte { repeated int64 element = 1;}" +
            "message Byte { repeated int32 element = 1;}" +
            "message short { repeated int32 element = 1;}" +
            "message Short { repeated int32 element = 1;}" +
            "message int { repeated int32 element = 1;}" +
            "message Integer { repeated int32 element = 1;}" +
            "message long { repeated int64 element = 1;}" +
            "message Long { repeated int64 element = 1;}" +
            "message double { repeated double element = 1;}" +
            "message Double { repeated double element = 1;}" +
            "message String { repeated string element = 1;}";
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext serCtx) {
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(byte.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(Byte.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(short.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(Short.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(int.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(Integer.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(long.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(Long.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(double.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(Double.class));
      serCtx.registerMarshaller(new PrimitiveArrayMarshaller(String.class));
   }
}
