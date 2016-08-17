package org.infinispan.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * BeanConventions.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class BeanConventions {

   public static String getPropertyFromBeanConvention(Method method) {
      String getterOrSetter = method.getName();
      if (getterOrSetter.startsWith("get") || getterOrSetter.startsWith("set")) {
         String withoutGet = getterOrSetter.substring(4);
         // not specifically BEAN convention, but this is what is bound in JMX.
         return Character.toUpperCase(getterOrSetter.charAt(3)) + withoutGet;
      } else if (getterOrSetter.startsWith("is")) {
         String withoutIs = getterOrSetter.substring(3);
         return Character.toUpperCase(getterOrSetter.charAt(2)) + withoutIs;
      }
      return getterOrSetter;
   }

   public static String getPropertyFromBeanConvention(Field field) {
      String fieldName = field.getName();
      String withoutFirstChar = fieldName.substring(1);
      return Character.toUpperCase(fieldName.charAt(0)) + withoutFirstChar;
   }

}
