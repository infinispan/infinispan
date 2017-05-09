package org.infinispan.scripting.utils;

import java.util.Arrays;
import java.util.stream.Stream;

import jdk.nashorn.api.scripting.ScriptUtils;

public class JSArrays {
   @SuppressWarnings("restriction")
   public static Stream<Object> stream(Object array) {
      Object[] objectArray = (Object[]) ScriptUtils.convert(array, Object[].class);
      return Arrays.stream(objectArray);
   }
}
