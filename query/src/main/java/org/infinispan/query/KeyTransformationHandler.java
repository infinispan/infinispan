package org.infinispan.query;

import org.infinispan.util.Util;
import org.infinispan.CacheException;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class KeyTransformationHandler {
   public static Object stringToKey(String s) {
      char type = s.charAt(0);
      switch (type) {
         case 'S':
            // this is a normal String
            return s.substring(2);
         case 'I':
            // This is an Integer
            return Integer.parseInt(s.substring(2));
         case 'T':
            // this is a custom transformable.
            int indexOfSecondDelimiter = s.indexOf(":", 2);
            String transformerClassName = s.substring(2, indexOfSecondDelimiter);
            String keyAsString = s.substring(indexOfSecondDelimiter + 1);
            Transformer t = null;
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
      if (validKey(key)) {
         // this string should be in the format of
         // "<TYPE>:(TRANSFORMER):<KEY>"
         // e.g.:
         //   "S:my string key"
         //   "I:75"
         //   "D:5.34"
         //   "B:f"
         //   "T:com.myorg.MyTransformer:STRING_GENERATED_BY_MY_TRANSFORMER"


         // for now just support Strings!!
         return "S:" + key.toString();
      }
      else
         throw new IllegalArgumentException("Indexing only works with entries keyed on Strings - you passed in a " + key.getClass().toString());
   }

   private static boolean validKey(Object key) {
      // for now we just support Strings and bypass the rest of the logic!
      if (true) return key instanceof String;
      
      // we support String and JDK primitives and their wrappers.
      if (key instanceof String ||
            key instanceof Integer ||
            key instanceof Long ||
            key instanceof Float ||
            key instanceof Double ||
            key instanceof Boolean ||
            key instanceof Short ||
            key instanceof Byte
            )
         return true;
      if (key.getClass().isAnnotationPresent(Transformable.class))
         return true;

      return false;
   }

}
