package org.infinispan.query.backend;

import org.testng.annotations.Test;
import org.infinispan.query.test.CustomKey;

/**
 * This is the test class for {@link org.infinispan.query.backend.KeyTransformationHandler}
 *
 * @author Navin Surtani
 */

@Test(groups = "functional")
public class KeyTransformationHandlerTest {

   String s = null;
   Object key = null;

   public void testKeyToStringWithStringAndPrimitives() {
      s = KeyTransformationHandler.keyToString("key");
      assert s.equals("S:key");

      s = KeyTransformationHandler.keyToString(1);
      assert s.equals("I:1");

      s = KeyTransformationHandler.keyToString(true);
      assert s.equals("B:true");

      s = KeyTransformationHandler.keyToString((short) 1);
      assert s.equals("X:1");

      s = KeyTransformationHandler.keyToString((long) 1);
      assert s.equals("L:1");

      s = KeyTransformationHandler.keyToString((byte) 1);
      assert s.equals("Y:1");

      s = KeyTransformationHandler.keyToString((float) 1);
      assert s.equals("F:1.0");

      s = KeyTransformationHandler.keyToString('A');
      assert s.equals("C:A");

      s = KeyTransformationHandler.keyToString(1.0);
      assert s.equals("D:1.0");
   }

   public void testStringToKeyWithStringAndPrimitives() {
      key = KeyTransformationHandler.stringToKey("S:key1");
      assert key.getClass().equals(String.class);
      assert key.equals("key1");

      key = KeyTransformationHandler.stringToKey("I:2");
      assert key.getClass().equals(Integer.class);
      assert key.equals(2);

      key = KeyTransformationHandler.stringToKey("Y:3");
      assert key.getClass().equals(Byte.class);
      assert key.equals((byte) 3);

      key = KeyTransformationHandler.stringToKey("F:4.0");
      assert key.getClass().equals(Float.class);
      assert key.equals((float) 4.0);

      key = KeyTransformationHandler.stringToKey("L:5");
      assert key.getClass().equals(Long.class);
      assert key.equals((long) 5);

      key = KeyTransformationHandler.stringToKey("X:6");
      assert key.getClass().equals(Short.class);
      assert key.equals((short) 6);

      key = KeyTransformationHandler.stringToKey("B:true");
      assert key.getClass().equals(Boolean.class);
      assert key.equals(true);

      key = KeyTransformationHandler.stringToKey("D:8.0");
      assert key.getClass().equals(Double.class);
      assert key.equals(8.0);

      key = KeyTransformationHandler.stringToKey("C:9");
      assert key.getClass().equals(Character.class);
      assert key.equals('9');

   }

   public void testStringToKeyWithCustomTransformable(){
      CustomKey customKey = new CustomKey("hello", 5);
      System.out.println(customKey.getClass().getName());
   }
}
