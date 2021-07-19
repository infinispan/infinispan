package org.infinispan.scripting.utils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

public class JSArrays {
   static final Method SCRIPTUTILS_CONVERT;

   static {
      Class<?> SCRIPTUTILS;
      try {
         SCRIPTUTILS = Class.forName("org.openjdk.nashorn.api.scripting.ScriptUtils");
      } catch (ClassNotFoundException e) {
         try {
            SCRIPTUTILS = Class.forName("jdk.nashorn.api.scripting.ScriptUtils");
         } catch (ClassNotFoundException classNotFoundException) {
            throw new RuntimeException(classNotFoundException);
         }
      }
      try {
         SCRIPTUTILS_CONVERT = SCRIPTUTILS.getMethod("convert", Object.class, Object.class);
      } catch (NoSuchMethodException e) {
         throw new RuntimeException(e);
      }
   }

   @SuppressWarnings("restriction")
   public static Stream<Object> stream(Object array) {
      try {
         return Arrays.stream((Object[]) SCRIPTUTILS_CONVERT.invoke(null, array, Object[].class));
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }
}
