package org.infinispan.objectfilter.impl.util;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionHelperTest {

   private static class Base {
   }

   private static abstract class Y extends Throwable implements Comparable<String>, List<Long> {
   }

   private static abstract class Z extends Y {
   }

   private static abstract class Q extends Throwable implements Map<String, Double> {
   }

   private static abstract class W extends Q {
   }

   private static class X<T extends Base> {
      T[] arr;
      Float[] arr2;
      float[] arr3;
      List<Integer> list;
      List<List<Integer>> list2;
      Map<String, Integer> map;
      Map<T, T> map2;
      Y y;
      Z z;
      Q q;
      Q w;
   }

   @Test
   public void testGetElementType() throws Exception {
      assertEquals(Base.class, ReflectionHelper.getElementType(X.class, "arr"));
      assertEquals(Float.class, ReflectionHelper.getElementType(X.class, "arr2"));
      assertEquals(float.class, ReflectionHelper.getElementType(X.class, "arr3"));
      assertEquals(Integer.class, ReflectionHelper.getElementType(X.class, "list"));
      assertEquals(List.class, ReflectionHelper.getElementType(X.class, "list2"));
      assertEquals(Integer.class, ReflectionHelper.getElementType(X.class, "map"));
      assertEquals(Base.class, ReflectionHelper.getElementType(X.class, "map2"));
      assertEquals(Long.class, ReflectionHelper.getElementType(X.class, "y"));
      assertEquals(Long.class, ReflectionHelper.getElementType(X.class, "z"));
      assertEquals(Double.class, ReflectionHelper.getElementType(X.class, "q"));
      assertEquals(Double.class, ReflectionHelper.getElementType(X.class, "w"));
   }
}