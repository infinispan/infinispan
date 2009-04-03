package org.horizon.loader.jdbc.stringbased;

/**
 * Default implementation for {@link org.horizon.loader.jdbc.stringbased.Key2StringMapper}. It supports all the
 * primitive wrappers(e.g. Integer, Long etc).
 *
 * @author Mircea.Markus@jboss.com
 */
public class DefaultKey2StringMapper implements Key2StringMapper {

   /**
    * Returns true if this is an primitive wrapper, false otherwise.
    */
   public boolean isSupportedType(Class key) {
      return key == String.class ||
            key == Short.class ||
            key == Byte.class ||
            key == Long.class ||
            key == Integer.class ||
            key == Double.class ||
            key == Float.class ||
            key == Boolean.class;
   }

   /**
    * Returns key.toString. As key being a primitive wrapper, this will ensure that it is unique.
    */
   public String getStringMapping(Object key) {
      if (key == null) {
         throw new NullPointerException("Not supporting null keys");
      }
      return key.toString();
   }
}
