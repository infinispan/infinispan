package org.infinispan.loaders.jdbc.stringbased;

import org.testng.annotations.Test;

/**
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "jdbc.stringbased.DefaultTwoWayKey2StringMapperTest")
public class DefaultTwoWayKey2StringMapperTest {

   DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();

   public void testAssumption() {
      //even if they have the same value, they have a different type
      assert ! new Float(3.0f).equals(new Integer(3));
   }

   public void testString() {
      assert mapper.isSupportedType(String.class);
      assert assertWorks("");
      assert assertWorks("mircea");
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
