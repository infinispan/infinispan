package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

final class PrimitiveExternalizer implements AdvancedExternalizer<Object> {

   // 0x00-0x0F reserved
   public static final int NULL = 0x00;

   private static final int BOOLEAN_PRIM = 0x10;
   private static final int BYTE_PRIM = 0x11;
   private static final int CHAR_PRIM = 0x12;
   private static final int DOUBLE_PRIM = 0x13;
   private static final int FLOAT_PRIM = 0x14;
   private static final int INT_PRIM = 0x15;
   private static final int LONG_PRIM = 0x16;
   private static final int SHORT_PRIM = 0x17;

   // 0x19 unused

   private static final int BOOLEAN_OBJ = 0x20;
   private static final int BYTE_OBJ = 0x21;
   private static final int CHAR_OBJ= 0x22;
   private static final int DOUBLE_OBJ = 0x23;
   private static final int FLOAT_OBJ = 0x24;
   private static final int INT_OBJ = 0x25;
   private static final int LONG_OBJ = 0x26;
   private static final int SHORT_OBJ = 0x27;

   private static final int STRING = 0x30;

   private final IdentityIntMap<Class<?>> subIds = new IdentityIntMap<>(16);
   private final Set<Class<?>> types = new PrimitiveTypes();

   public PrimitiveExternalizer() {
      subIds.put(boolean.class, BOOLEAN_PRIM); // for arrays
      subIds.put(byte.class, BYTE_PRIM);
      subIds.put(char.class, CHAR_PRIM);
      subIds.put(double.class, DOUBLE_PRIM);
      subIds.put(float.class, FLOAT_PRIM);
      subIds.put(int.class, INT_PRIM);
      subIds.put(long.class, LONG_PRIM);
      subIds.put(short.class, SHORT_PRIM);

      subIds.put(Boolean.class, BOOLEAN_OBJ);
      subIds.put(Byte.class, BYTE_OBJ);
      subIds.put(Character.class, CHAR_OBJ);
      subIds.put(Double.class, DOUBLE_OBJ);
      subIds.put(Float.class, FLOAT_OBJ);
      subIds.put(Integer.class, INT_OBJ);
      subIds.put(Long.class, LONG_OBJ);
      subIds.put(Short.class, SHORT_OBJ);

      subIds.put(String.class, STRING);
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
            case STRING:
               out.writeUTF((String) obj);
               break;
            default:
               throw new IOException("Unknown primitive type: " + obj);
         }
      }
   }

   @Override
   public Object readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      int subId = in.readUnsignedByte();
      switch (subId) {
         case NULL:
            return null;
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
         case STRING:
            return in.readUTF();
         default:
            throw new IOException("Unknown primitive sub id: " + Integer.toHexString(subId));
      }
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
