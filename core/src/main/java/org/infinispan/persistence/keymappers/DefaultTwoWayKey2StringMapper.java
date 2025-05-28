package org.infinispan.persistence.keymappers;

import java.util.Base64;
import java.util.UUID;

import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * Default implementation for {@link TwoWayKey2StringMapper} that knows how to handle all primitive
 * wrapper keys and Strings.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 *
 * @since 4.1
 */
public class DefaultTwoWayKey2StringMapper implements TwoWayKey2StringMapper {

   private static final char NON_STRING_PREFIX = '\uFEFF';
   private static final char SHORT_IDENTIFIER = '1';
   private static final char BYTE_IDENTIFIER = '2';
   private static final char LONG_IDENTIFIER = '3';
   private static final char INTEGER_IDENTIFIER = '4';
   private static final char DOUBLE_IDENTIFIER = '5';
   private static final char FLOAT_IDENTIFIER = '6';
   private static final char BOOLEAN_IDENTIFIER = '7';
   private static final char BYTEARRAYKEY_IDENTIFIER = '8';
   private static final char NATIVE_BYTEARRAYKEY_IDENTIFIER = '9';
   private static final char UUID_IDENTIFIER = 'a';

   @Override
   public String getStringMapping(Object key) {
      char identifier;
      if (key.getClass() == String.class) {
         return key.toString();
      } else if (key.getClass() == Short.class) {
         identifier = SHORT_IDENTIFIER;
      } else if (key.getClass() == Byte.class) {
         identifier = BYTE_IDENTIFIER;
      } else if (key.getClass() == Long.class) {
         identifier = LONG_IDENTIFIER;
      } else if (key.getClass() == Integer.class) {
         identifier = INTEGER_IDENTIFIER;
      } else if (key.getClass() == Double.class) {
         identifier = DOUBLE_IDENTIFIER;
      } else if (key.getClass() == Float.class) {
         identifier = FLOAT_IDENTIFIER;
      } else if (key.getClass() == Boolean.class) {
         identifier = BOOLEAN_IDENTIFIER;
      } else if (key.getClass() == WrappedByteArray.class) {
         return generateString(BYTEARRAYKEY_IDENTIFIER, Base64.getEncoder().encodeToString(((WrappedByteArray)key).getBytes()));
      } else if (key.getClass() == byte[].class) {
         return generateString(NATIVE_BYTEARRAYKEY_IDENTIFIER, Base64.getEncoder().encodeToString((byte[]) key));
      } else if (key.getClass() == UUID.class) {
         identifier = UUID_IDENTIFIER;
      } else {
         throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
      }
      return generateString(identifier, key.toString());
   }

   @Override
   public Object getKeyMapping(String key) {
      if (!key.isEmpty() && key.charAt(0) == NON_STRING_PREFIX) {
         char type = key.charAt(1);
         String value = key.substring(2);
         switch (type) {
            case SHORT_IDENTIFIER:
               return Short.parseShort(value);
            case BYTE_IDENTIFIER:
               return Byte.parseByte(value);
            case LONG_IDENTIFIER:
               return Long.parseLong(value);
            case INTEGER_IDENTIFIER:
               return Integer.parseInt(value);
            case DOUBLE_IDENTIFIER:
               return Double.parseDouble(value);
            case FLOAT_IDENTIFIER:
               return Float.parseFloat(value);
            case BOOLEAN_IDENTIFIER:
               return Boolean.parseBoolean(value);
            case BYTEARRAYKEY_IDENTIFIER:
               byte[] bytes = Base64.getDecoder().decode(value);
               return new WrappedByteArray(bytes);
            case NATIVE_BYTEARRAYKEY_IDENTIFIER:
               return Base64.getDecoder().decode(value);
            case UUID_IDENTIFIER:
               return UUID.fromString(value);
            default:
               throw new IllegalArgumentException("Unsupported type code: " + type);
         }
      } else {
         return key;
      }
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return isPrimitive(keyType) || keyType == WrappedByteArray.class|| keyType == UUID.class;
   }

   private String generateString(char identifier, String s) {
      return NON_STRING_PREFIX + String.valueOf(identifier) + s;
   }

   private static boolean isPrimitive(Class<?> key) {
      return key == String.class || key == Short.class || key == Byte.class || key == Long.class || key == Integer.class
            || key == Double.class || key == Float.class || key == Boolean.class || key == byte[].class;
   }
}
