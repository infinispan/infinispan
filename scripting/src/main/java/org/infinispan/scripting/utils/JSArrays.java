package org.infinispan.scripting.utils;

import java.util.Arrays;
import java.util.stream.Stream;

public class JSArrays {
   @SuppressWarnings("restriction")
   public static <T> Stream<T> stream(jdk.nashorn.internal.objects.NativeArray array) {
      T[] objectArray = (T[]) array.asObjectArray();
      return Arrays.stream(objectArray, 0, objectArray.length);
   }
}
