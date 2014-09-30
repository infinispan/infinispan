package org.infinispan.cdi.util;

import java.lang.reflect.Type;

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
      else if (type.equals(Boolean.TYPE))
      {
         return Boolean.class;
      }
      else if (type.equals(Character.TYPE))
      {
         return Character.class;
      }
      else if (type.equals(Byte.TYPE))
      {
         return Byte.class;
      }
      else if (type.equals(Short.TYPE))
      {
         return Short.class;
      }
      else if (type.equals(Integer.TYPE))
      {
         return Integer.class;
      }
      else if (type.equals(Long.TYPE))
      {
         return Long.class;
      }
      else if (type.equals(Float.TYPE))
      {
         return Float.class;
      }
      else if (type.equals(Double.TYPE))
      {
         return Double.class;
      }
      else if (type.equals(Void.TYPE))
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