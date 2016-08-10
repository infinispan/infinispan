package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

final class PrimitiveExternalizer implements AdvancedExternalizer<Object> {

   // TODO: Implement support for other primitive arrays

   // 0x03-0x0F reserved
   static final int NULL                       = 0x00;
   static final int BYTE_ARRAY                 = 0x01; // byte[].class
   static final int STRING                     = 0x02; // String.class

//   static final int BOOLEAN_PRIM                = 0x10; // boolean.class
//   static final int BYTE_PRIM                   = 0x11; // ..etc..
//   static final int CHAR_PRIM                   = 0x12;
//   static final int DOUBLE_PRIM                 = 0x13;
//   static final int FLOAT_PRIM                  = 0x14;
//   static final int INT_PRIM                    = 0x15;
//   static final int LONG_PRIM                   = 0x16;
//   static final int SHORT_PRIM                  = 0x17;

   static final int BOOLEAN_ARRAY               = 0x18; // boolean[].class
   static final int CHAR_ARRAY                  = 0x19; // ..etc..
   static final int DOUBLE_ARRAY                = 0x1A;
   static final int FLOAT_ARRAY                 = 0x1B;
   static final int INT_ARRAY                   = 0x1C;
   static final int LONG_ARRAY                  = 0x1D;
   static final int SHORT_ARRAY                 = 0x1E;

   // 0x1F unused

   static final int BOOLEAN_OBJ                 = 0x20; // Boolean.class
   static final int BYTE_OBJ                    = 0x21; // ..etc..
   static final int CHAR_OBJ                    = 0x22;
   static final int DOUBLE_OBJ                  = 0x23;
   static final int FLOAT_OBJ                   = 0x24;
   static final int INT_OBJ                     = 0x25;
   static final int LONG_OBJ                    = 0x26;
   static final int SHORT_OBJ                   = 0x27;

   static final int ARRAY_EMPTY                 = 0x28; // zero elements
   static final int ARRAY_SMALL                 = 0x29; // <=0x100 elements
   static final int ARRAY_MEDIUM                = 0x2A; // <=0x10000 elements
   static final int ARRAY_LARGE                 = 0x2B; // <0x80000000 elements

   // 0x2C-0x2F unused

   final IdentityIntMap<Class<?>> subIds = new IdentityIntMap<>(16);
   final Set<Class<?>> types = new PrimitiveTypes();

   // Assumes that whatever encoding passed can handle ObjectOutput/ObjectInput
   // (that might change when adding support for NIO ByteBuffers)
   final Encoding enc;

   public PrimitiveExternalizer(Encoding enc) {
      this.enc = enc;
      subIds.put(String.class, STRING);
      subIds.put(byte[].class, BYTE_ARRAY);

//      subIds.put(boolean.class, BOOLEAN_PRIM); // for arrays
//      subIds.put(byte.class, BYTE_PRIM);
//      subIds.put(char.class, CHAR_PRIM);
//      subIds.put(double.class, DOUBLE_PRIM);
//      subIds.put(float.class, FLOAT_PRIM);
//      subIds.put(int.class, INT_PRIM);
//      subIds.put(long.class, LONG_PRIM);
//      subIds.put(short.class, SHORT_PRIM);

      subIds.put(boolean[].class, BOOLEAN_ARRAY);
      subIds.put(char[].class, CHAR_ARRAY);
      subIds.put(double[].class, DOUBLE_ARRAY);
      subIds.put(float[].class, FLOAT_ARRAY);
      subIds.put(int[].class, INT_ARRAY);
      subIds.put(long[].class, LONG_ARRAY);
      subIds.put(short[].class, SHORT_ARRAY);

      subIds.put(Boolean.class, BOOLEAN_OBJ);
      subIds.put(Byte.class, BYTE_OBJ);
      subIds.put(Character.class, CHAR_OBJ);
      subIds.put(Double.class, DOUBLE_OBJ);
      subIds.put(Float.class, FLOAT_OBJ);
      subIds.put(Integer.class, INT_OBJ);
      subIds.put(Long.class, LONG_OBJ);
      subIds.put(Short.class, SHORT_OBJ);

   }

   @Override
   public Set<Class<?>> getTypeClasses() {
      return types;
   }

   @Override
   public void writeObject(ObjectOutput out, Object obj) throws IOException {
      if (obj == null) {
         out.writeByte(NULL);
      } else {
         int subId = subIds.get(obj.getClass(), -1);
         out.writeByte(subId);
         switch (subId) {
            case BYTE_ARRAY:
               writeByteArray((byte[]) obj, out);
               break;
            case STRING:
               writeString(out, (String) obj);
               break;
            case BOOLEAN_OBJ:
               out.writeBoolean((boolean) obj);
               break;
            case BYTE_OBJ:
               out.writeByte((int) obj);
               break;
            case CHAR_OBJ:
               out.writeChar((int) obj);
               break;
            case DOUBLE_OBJ:
               out.writeDouble((double) obj);
               break;
            case FLOAT_OBJ:
               out.writeFloat((float) obj);
               break;
            case INT_OBJ:
               out.writeInt((int) obj);
               break;
            case LONG_OBJ:
               out.writeLong((long) obj);
               break;
            case SHORT_OBJ:
               out.writeShort((int) obj);
               break;
            default:
               throw new IOException("Unknown primitive type: " + obj);
         }
      }
   }

   private void writeByteArray(byte[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
//         out.writeByte(BYTE_PRIM);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
//         out.writeByte(BYTE_PRIM);
         out.write(obj, 0, len);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
//         out.writeByte(BYTE_PRIM);
         out.write(obj, 0, len);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
//         out.writeByte(BYTE_PRIM);
         out.write(obj, 0, len);
      }
   }

   @SuppressWarnings("unchecked")
   private void writeString(ObjectOutput out, String obj) {
      // Instead of out.writeUTF() to be able to write smaller String payloads
      enc.encodeString(obj, out);
   }

   private void writeBooleanArray(boolean[] obj) {
      // TODO: Customise this generated block
   }

   @Override
   public Object readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      int subId = in.readUnsignedByte();
      switch (subId) {
         case NULL:
            return null;
         case STRING:
            return readString(in);
         case BYTE_ARRAY:
            return readByteArray(in);
         case BOOLEAN_OBJ:
            return in.readBoolean();
         case BYTE_OBJ:
            return in.readByte();
         case CHAR_OBJ:
            return in.readChar();
         case DOUBLE_OBJ:
            return in.readDouble();
         case FLOAT_OBJ:
            return in.readFloat();
         case INT_OBJ:
            return in.readInt();
         case LONG_OBJ:
            return in.readLong();
         case SHORT_OBJ:
            return in.readShort();
         default:
            throw new IOException("Unknown primitive sub id: " + Integer.toHexString(subId));
      }
   }

   @SuppressWarnings("unchecked")
   private String readString(ObjectInput in) {
      return enc.decodeString(in); // Counterpart to Encoding.encodeString()
   }

   private Object readByteArray(ObjectInput in) throws IOException {
      byte arrayType = in.readByte();
      switch (arrayType) {
         case ARRAY_EMPTY:
            return new byte[]{};
         case ARRAY_SMALL:
            byte lenB = in.readByte();
            byte[] bytesB = new byte[lenB];
            in.readFully(bytesB);
            return bytesB;
         case ARRAY_MEDIUM:
            short lenS = in.readShort();
            byte[] bytesS = new byte[lenS];
            in.readFully(bytesS);
            return bytesS;
         case ARRAY_LARGE:
            int lenI = in.readInt();
            byte[] bytesI = new byte[lenI];
            in.readFully(bytesI);
            return bytesI;
      }

      return null;  // TODO: Customise this generated block
   }

   @Override
   public Integer getId() {
      return InternalIds.PRIMITIVE;
   }

   private final class PrimitiveTypes extends AbstractSet<Class<?>> {

      @Override
      public boolean contains(Object o) {
         final int doesNotExist = -1;
         int i = PrimitiveExternalizer.this.subIds.get((Class<?>) o, doesNotExist);
         return i != doesNotExist;
      }

      @Override
      public Iterator<Class<?>> iterator() {
         return null;
      }

      @Override
      public int size() {
         return 0;
      }

   }

}
