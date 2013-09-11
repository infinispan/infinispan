package org.infinispan.persistence.keymappers;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.keymappers.DefaultTwoWayKey2StringMapperTest")
public class DefaultTwoWayKey2StringMapperTest {

   DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();

   public void testKeyMapper() {
      String skey = mapper.getStringMapping("k1");
      assert skey.equals("k1");

      skey = mapper.getStringMapping(Integer.valueOf(100));

      assert !skey.equals("100");

      Integer i = (Integer) mapper.getKeyMapping(skey);
      assert i == 100;

      skey = mapper.getStringMapping(Boolean.TRUE);

      assert !skey.equalsIgnoreCase("true");

      Boolean b = (Boolean) mapper.getKeyMapping(skey);

      assert b;

      skey = mapper.getStringMapping(Double.valueOf(3.141592d));

      assert !skey.equals("3.141592");

      Double d = (Double) mapper.getKeyMapping(skey);

      assert d == 3.141592d;
   }

   public void testPrimitivesAreSupported() {
      assert mapper.isSupportedType(Integer.class);
      assert mapper.isSupportedType(Byte.class);
      assert mapper.isSupportedType(Short.class);
      assert mapper.isSupportedType(Long.class);
      assert mapper.isSupportedType(Double.class);
      assert mapper.isSupportedType(Float.class);
      assert mapper.isSupportedType(Boolean.class);
      assert mapper.isSupportedType(String.class);
   }

   public void testTwoWayContract() {
      Object[] toTest = { 0, new Byte("1"), new Short("2"), (long) 3, new Double("3.4"), new Float("3.5"), Boolean.FALSE, "some string" };
      for (Object o : toTest) {
         Class<?> type = o.getClass();
         String rep = mapper.getStringMapping(o);
         assert o.equals(mapper.getKeyMapping(rep)) : String.format("Failed on type %s and value %s", type, rep);
      }
   }

   public void testAssumption() {
      // even if they have the same value, they have a different type
      assert !new Float(3.0f).equals(new Integer(3));
   }

   public void testString() {
      assert mapper.isSupportedType(String.class);
      assert assertWorks("") : "Expected empty string, was " + mapper.getStringMapping("");
      assert assertWorks("mircea") : "Expected 'mircea', was " + mapper.getStringMapping("mircea");
   }

   public void testShort() {
      assert mapper.isSupportedType(Short.class);
      assert assertWorks((short) 2);
   }

   public void testByte() {
      assert mapper.isSupportedType(Byte.class);
      assert assertWorks((byte) 2);
   }

   public void testLong() {
      assert mapper.isSupportedType(Long.class);
      assert assertWorks(new Long(2));
   }

   public void testInteger() {
      assert mapper.isSupportedType(Integer.class);
      assert assertWorks(2);
   }

   public void testDouble() {
      assert mapper.isSupportedType(Double.class);
      assert assertWorks(2.4d);

   }

   public void testFloat() {
      assert mapper.isSupportedType(Float.class);
      assert assertWorks(2.1f);

   }

   public void testBoolean() {
      assert mapper.isSupportedType(Boolean.class);
      assert assertWorks(true);
      assert assertWorks(false);
   }

   private boolean assertWorks(Object key) {
      return mapper.getKeyMapping(mapper.getStringMapping(key)).equals(key);
   }
}
