package org.infinispan.cdi.common.util;

/**
 * Utility class for Types
 *
 * @author Pete Muir
 */
public class Types
{

   public static Class<?> boxedClass(Class<?> type)
   {
      if (!type.isPrimitive())
      {
         return type;
      }
      else if (type == Boolean.TYPE)
      {
         return Boolean.class;
      }
      else if (type == Character.TYPE)
      {
         return Character.class;
      }
      else if (type == Byte.TYPE)
      {
         return Byte.class;
      }
      else if (type == Short.TYPE)
      {
         return Short.class;
      }
      else if (type == Integer.TYPE)
      {
         return Integer.class;
      }
      else if (type == Long.TYPE)
      {
         return Long.class;
      }
      else if (type == Float.TYPE)
      {
         return Float.class;
      }
      else if (type == Double.TYPE)
      {
         return Double.class;
      }
      else if (type == Void.TYPE)
      {
         return Void.class;
      }
      else
      {
         // Vagaries of if/else statement, can't be reached ;-)
         return type;
      }
   }
}
