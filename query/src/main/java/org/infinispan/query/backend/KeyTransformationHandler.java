package org.infinispan.query.backend;

import org.infinispan.util.Util;
import org.infinispan.CacheException;
import org.infinispan.query.Transformer;
import org.infinispan.query.Transformable;

/**
 * This transforms arbitrary keys to a String which can be used by Lucene as a document identifier, and vice versa.
 * <p/>
 * There are 2 approaches to doing so; one for SimpleKeys: Java primitives (and their object wrappers) and Strings, and
 * one for custom, user-defined types that could be used as keys.
 * <p/>
 * For SimpleKeys, users don't need to do anything, these keys are automatically transformed by this class.
 * <p/>
 * For user-defined keys, only types annotated with @Transformable, and declaring an appropriate {@link
 * org.infinispan.query.Transformer} implementation, are supported.
 *
 * @author Manik Surtani
 * @see org.infinispan.query.Transformable
 * @see org.infinispan.query.Transformer
 * @since 4.0
 */
public class KeyTransformationHandler {
   public static Object stringToKey(String s) {
      char type = s.charAt(0);
      switch (type) {
         case 'S':
            // this is a normal String, but NOT a SHORT. For short see case 'x'.
            return s.substring(2);
         case 'I':
            // This is an Integer
            return Integer.parseInt(s.substring(2));
         case 'Y':
            // This is a BYTE
            return Byte.parseByte(s.substring(2));
         case 'L':
            // This is a Long
            return Long.parseLong(s.substring(2));
         case 'X':
            // This is a SHORT
            return Short.parseShort(s.substring(2));
         case 'D':
            // This is a Double
            return Double.parseDouble(s.substring(2));
         case 'F':
            // This is a Float
            return Float.parseFloat(s.substring(2));
         case 'B':
            // This is a Boolean. This is NOT the case for a BYTE. For a BYTE, see case 'y'.
            return Boolean.parseBoolean(s.substring(2));
         case 'C':
            // This is a Character
            return s.charAt(2);
         case 'T':
            // this is a custom transformable.
            int indexOfSecondDelimiter = s.indexOf(":", 2);
            String transformerClassName = s.substring(2, indexOfSecondDelimiter);
            String keyAsString = s.substring(indexOfSecondDelimiter + 1);
            Transformer t;
            // try and locate class
            try {
               t = (Transformer) Util.getInstance(transformerClassName);
            } catch (Exception e) {
               // uh oh, cannot load this class!  What now?
               throw new CacheException(e);
            }

            return t.fromString(keyAsString);
      }
      throw new CacheException("Unknown type metadata " + type);
   }

   public static String keyToString(Object key) {
      // this string should be in the format of
      // "<TYPE>:(TRANSFORMER):<KEY>"
      // e.g.:
      //   "S:my string key"
      //   "I:75"
      //   "D:5.34"
      //   "B:f"
      //   "T:com.myorg.MyTransformer:STRING_GENERATED_BY_MY_TRANSFORMER"

      char prefix = ' ';

      // First going to check if the key is a primitive or a String. Otherwise, check if it's a transformable.
      // If none of those conditions are satisfied, we'll throw an Exception.

      if (isStringOrPrimitive(key)) {
         // Using 'X' for Shorts and 'Y' for Bytes because 'S' is used for Strings and 'B' is being used for Booleans.


         if (key instanceof String)
            prefix = 'S';
         else if (key instanceof Integer)
            prefix = 'I';
         else if (key instanceof Boolean)
            prefix = 'B';
         else if (key instanceof Long)
            prefix = 'L';
         else if (key instanceof Float)
            prefix = 'F';
         else if (key instanceof Double)
            prefix = 'D';
         else if (key instanceof Short)
            prefix = 'X';
         else if (key instanceof Byte)
            prefix = 'Y';
         else if (key instanceof Character)
            prefix = 'C';

         return prefix + ":" + key;

      } else if (isTransformable(key)) {
         // There is a bit more work to do for this case.
         prefix = 'T';

         System.out.println("key class is: - " + key.getClass());
         // Do the transformer casting

         // Try and get the @Transformable annotation.
         Transformable transformableAnnotation = key.getClass().getAnnotation(Transformable.class);

         // Use that to find the class that is being used as the transformer.
         Class<? extends Transformer> transformerClass = transformableAnnotation.transformer();
         Transformer t;
         try {
            t = Util.getInstance(transformerClass);
         }
         catch (Exception e) {
            throw new CacheException(e);
         }
         //Get the name of the Class that has been used. Add it to the toString() method that has to be defined
         // in the Transformer implementation
         String subKey = key.getClass().getName() + ":" + t.toString(key);
         // We've built the second part of the String and now need to add that bit to the prefix for our complete keyString
         // for lucene.
         return prefix + ":" + subKey;

      } else
         throw new IllegalArgumentException("Indexing only works with entries keyed on Strings, primitives " +
               "and classes that have the @Transformable annotation - you passed in a " + key.getClass().toString());
   }

   private static boolean isStringOrPrimitive(Object key) {

      // we support String and JDK primitives and their wrappers.
      if (key instanceof String ||
            key instanceof Integer ||
            key instanceof Long ||
            key instanceof Float ||
            key instanceof Double ||
            key instanceof Boolean ||
            key instanceof Short ||
            key instanceof Byte ||
            key instanceof Character
            )
         return true;

      return false;
   }

   private static Boolean isTransformable(Object key) {
      // returns true if the Transformable annotation is present on the custom key class. 
      return key.getClass().isAnnotationPresent(Transformable.class);

   }

}
