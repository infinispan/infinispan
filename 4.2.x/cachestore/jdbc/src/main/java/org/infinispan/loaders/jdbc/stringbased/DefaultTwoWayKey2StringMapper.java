package org.infinispan.loaders.jdbc.stringbased;

/**
 * Default implementation for {@link org.infinispan.loaders.jdbc.stringbased.TwoWayKey2StringMapper} that knows how to
 * handle all primitive wrapper keys and Strings.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultTwoWayKey2StringMapper implements TwoWayKey2StringMapper {

   private static final String STRING_IDENTIFIER = "0";
   private static final String SHORT_IDENTIFIER = "1";
   private static final String BYTE_IDENTIFIER = "2";
   private static final String LONG_IDENTIFIER = "3";
   private static final String INTEGER_IDENTIFIER = "4";
   private static final String DOUBLE_IDENTIFIER = "5";
   private static final String FLOAT_IDENTIFIER = "6";
   private static final String BOOLEAN_IDENTIFIER = "7";


   @Override
   public String getStringMapping(Object key) {
      String identifier;
      if (key.getClass().equals(String.class)) {
         identifier = STRING_IDENTIFIER;
      } else if (key.getClass().equals(Short.class)) {
         identifier = SHORT_IDENTIFIER;
      } else if (key.getClass().equals(Byte.class)) {
         identifier = BYTE_IDENTIFIER;
      } else if (key.getClass().equals(Long.class)) {
         identifier = LONG_IDENTIFIER;
      } else if (key.getClass().equals(Integer.class)) {
         identifier = INTEGER_IDENTIFIER;
      } else if (key.getClass().equals(Double.class)) {
         identifier = DOUBLE_IDENTIFIER;
      } else if (key.getClass().equals(Float.class)) {
         identifier = FLOAT_IDENTIFIER;
      } else if (key.getClass().equals(Boolean.class)) {
         identifier = BOOLEAN_IDENTIFIER;
      } else {
         throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
      } 
      return generateString(identifier, key.toString());
   }

   @Override
   public Object getKeyMapping(String key) {
      String type = String.valueOf(key.charAt(0));
      String value = key.substring(1);
      if (type.equals(STRING_IDENTIFIER)) {
         return value;
      } else if (type.equals(SHORT_IDENTIFIER)) {
         return Short.parseShort(value);
      } else if (type.equals(BYTE_IDENTIFIER)) {
         return Byte.parseByte(value);
      } else if (type.equals(LONG_IDENTIFIER)) {
         return Long.parseLong(value);
      } else if (type.equals(INTEGER_IDENTIFIER)) {
         return Integer.parseInt(value);
      } else if (type.equals(DOUBLE_IDENTIFIER)) {
         return Double.parseDouble(value);
      } else if (type.equals(FLOAT_IDENTIFIER)) {
         return Float.parseFloat(value);
      } else if (type.equals(BOOLEAN_IDENTIFIER)) {
         return Boolean.parseBoolean(value);
      } else {
         throw new IllegalArgumentException("Unsupported type code: " + type);
      }
   }

   @Override
   public boolean isSupportedType(Class keyType) {
      return isPrimitive(keyType);
   }

   private String generateString(String identifier, String s) {
      return identifier + s;
   }

   static boolean isPrimitive(Class key) {
      return key == String.class ||
            key == Short.class ||
            key == Byte.class ||
            key == Long.class ||
            key == Integer.class ||
            key == Double.class ||
            key == Float.class ||
            key == Boolean.class;
   }
}
