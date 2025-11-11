package org.infinispan.scripting.utils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

public class JSArrays {
   static final Method SCRIPTUTILS_CONVERT;

   static {
      Class<?> SCRIPTUTILS;
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try {
         SCRIPTUTILS = Class.forName("org.openjdk.nashorn.api.scripting.ScriptUtils", true, loader);
      } catch (ClassNotFoundException e1) {
         try {
            SCRIPTUTILS = Class.forName("jdk.nashorn.api.scripting.ScriptUtils", true, loader);
         } catch (ClassNotFoundException e2) {
            RuntimeException rte = new RuntimeException("Cannot find Nashorn ScriptUtils");
            rte.addSuppressed(e1);
            rte.addSuppressed(e2);
            throw rte;
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
