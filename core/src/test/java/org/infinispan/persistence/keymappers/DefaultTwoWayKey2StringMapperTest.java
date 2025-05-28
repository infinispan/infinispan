package org.infinispan.persistence.keymappers;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.UUID;

import org.infinispan.commons.util.Util;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.keymappers.DefaultTwoWayKey2StringMapperTest")
public class DefaultTwoWayKey2StringMapperTest {

   DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();

   public void testKeyMapper() {
      String skey = mapper.getStringMapping("k1");
      assertEquals("k1", skey);

      skey = mapper.getStringMapping(100);
      assertNotEquals("100", skey);

      Integer i = (Integer) mapper.getKeyMapping(skey);
      assertEquals(100, i.intValue());

      skey = mapper.getStringMapping(Boolean.TRUE);
      assertNotEquals("true", skey);

      Boolean b = (Boolean) mapper.getKeyMapping(skey);

      assertTrue(b);

      skey = mapper.getStringMapping(3.141592d);
      assertNotEquals("3.141592", skey);

      Double d = (Double) mapper.getKeyMapping(skey);

      assertEquals(3.141592d, d);

      UUID uuid = Util.threadLocalRandomUUID();
      skey = mapper.getStringMapping(uuid);
      assertNotEquals(uuid.toString(), skey);

      UUID u = (UUID) mapper.getKeyMapping(skey);
      assertEquals(uuid, u);

   }

   public void testPrimitivesAreSupported() {
      assertTrue(mapper.isSupportedType(Integer.class));
      assertTrue(mapper.isSupportedType(Byte.class));
      assertTrue(mapper.isSupportedType(Short.class));
      assertTrue(mapper.isSupportedType(Long.class));
      assertTrue(mapper.isSupportedType(Double.class));
      assertTrue(mapper.isSupportedType(Float.class));
      assertTrue(mapper.isSupportedType(Boolean.class));
      assertTrue(mapper.isSupportedType(String.class));
   }

   public void testTwoWayContract() {
      Object[] toTest = {0, (byte) 1, (short) 2, 3.4, (float) 3.5, false, "some string"};
      for (Object o : toTest) {
         Class<?> type = o.getClass();
         String rep = mapper.getStringMapping(o);
         assertEquals(String.format("Failed on type %s and value %s", type, rep), o, mapper.getKeyMapping(rep));
      }
   }

   public void testString() {
      assertTrue(mapper.isSupportedType(String.class));
      assertTrue(assertWorks(""), "Expected empty string, was " + mapper.getStringMapping(""));
      assertTrue(assertWorks("mircea"), "Expected 'mircea', was " + mapper.getStringMapping("mircea"));
   }

   public void testShort() {
      assertTrue(mapper.isSupportedType(Short.class));
      assertTrue(assertWorks((short) 2));
   }

   public void testByte() {
      assertTrue(mapper.isSupportedType(Byte.class));
      assertTrue(assertWorks((byte) 2));
   }

   public void testLong() {
      assertTrue(mapper.isSupportedType(Long.class));
      assertTrue(assertWorks(2L));
   }

   public void testInteger() {
      assertTrue(mapper.isSupportedType(Integer.class));
      assertTrue(assertWorks(2));
   }

   public void testDouble() {
      assertTrue(mapper.isSupportedType(Double.class));
      assertTrue(assertWorks(2.4d));

   }

   public void testFloat() {
      assertTrue(mapper.isSupportedType(Float.class));
      assertTrue(assertWorks(2.1f));

   }

   public void testBoolean() {
      assertTrue(mapper.isSupportedType(Boolean.class));
      assertTrue(assertWorks(true));
      assertTrue(assertWorks(false));
   }

   public void testUuid() {
      assertTrue(mapper.isSupportedType(UUID.class));
      assertTrue(assertWorks(Util.threadLocalRandomUUID()));
   }

   private boolean assertWorks(Object key) {
      return mapper.getKeyMapping(mapper.getStringMapping(key)).equals(key);
   }
}
