package org.infinispan.query.backend;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Base64;
import java.util.UUID;

import org.infinispan.commons.CacheException;
import org.infinispan.query.Transformer;
import org.infinispan.query.test.CustomKey;
import org.infinispan.query.test.CustomKey2;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.NonSerializableKey;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is the test class for {@link org.infinispan.query.backend.KeyTransformationHandler}.
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */
@Test(groups = "functional", testName = "query.backend.KeyTransformationHandlerTest")
public class KeyTransformationHandlerTest {

   private KeyTransformationHandler keyTransformationHandler;

   private final UUID randomUUID = UUID.randomUUID();

   @BeforeMethod
   public void beforeMethod() {
      keyTransformationHandler = new KeyTransformationHandler(null);
   }

   public void testKeyToStringWithStringAndPrimitives() {
      String s = keyTransformationHandler.keyToString("key", 1);
      assert s.equals("S:key:1");

      s = keyTransformationHandler.keyToString(1, 12);
      assert s.equals("I:1:12");

      s = keyTransformationHandler.keyToString(true, 250);
      assert s.equals("B:true:250");

      s = keyTransformationHandler.keyToString((short) 1, 1);
      assert s.equals("X:1:1");

      s = keyTransformationHandler.keyToString((long) 1, 4);
      assert s.equals("L:1:4");

      s = keyTransformationHandler.keyToString((byte) 1, 3);
      assert s.equals("Y:1:3");

      s = keyTransformationHandler.keyToString((float) 1, 9);
      assert s.equals("F:1.0:9");

      s = keyTransformationHandler.keyToString('A', 11);
      assert s.equals("C:A:11");

      s = keyTransformationHandler.keyToString(1.0, 2);
      assert s.equals("D:1.0:2");

      s = keyTransformationHandler.keyToString(randomUUID, 0);
      assert s.equals("U:" + randomUUID + ":0");

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      s = keyTransformationHandler.keyToString(arr, 1);
      assert s.equals("A:" + Base64.getEncoder().encodeToString(arr) + ":1");
   }

   public void testStringToKeyWithStringAndPrimitives() {
      Object key = keyTransformationHandler.stringToKey("S:key1:0");
      assert key.getClass().equals(String.class);
      assert key.equals("key1");

      key = keyTransformationHandler.stringToKey("I:2:0");
      assert key.getClass().equals(Integer.class);
      assert key.equals(2);

      key = keyTransformationHandler.stringToKey("Y:3:0");
      assert key.getClass().equals(Byte.class);
      assert key.equals((byte) 3);

      key = keyTransformationHandler.stringToKey("F:4.0:0");
      assert key.getClass().equals(Float.class);
      assert key.equals((float) 4.0);

      key = keyTransformationHandler.stringToKey("L:5:0");
      assert key.getClass().equals(Long.class);
      assert key.equals((long) 5);

      key = keyTransformationHandler.stringToKey("X:6:0");
      assert key.getClass().equals(Short.class);
      assert key.equals((short) 6);

      key = keyTransformationHandler.stringToKey("B:true:0");
      assert key.getClass().equals(Boolean.class);
      assert key.equals(true);

      key = keyTransformationHandler.stringToKey("D:8.0:0");
      assert key.getClass().equals(Double.class);
      assert key.equals(8.0);

      key = keyTransformationHandler.stringToKey("C:9:0");
      assert key.getClass().equals(Character.class);
      assert key.equals('9');

      key = keyTransformationHandler.stringToKey("U:" + randomUUID + ":0");
      assert key.getClass().equals(UUID.class);
      assert key.equals(randomUUID);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      key = keyTransformationHandler.stringToKey("A:" + Base64.getEncoder().encodeToString(arr) + ":0");
      assertEquals(arr, (byte[]) key);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToUnknownKey() {
      keyTransformationHandler.stringToKey("Z:someKey:0");
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToKeyWithInvalidTransformer() {
      keyTransformationHandler.stringToKey("T:org.infinispan.InexistentTransformer:key1:0");
   }

   public void testStringToKeyWithCustomTransformable() {
      CustomKey customKey = new CustomKey(88, 8800, 12889976);
      String strRep = keyTransformationHandler.keyToString(customKey, 2);
      Object keyAgain = keyTransformationHandler.stringToKey(strRep);
      assertEquals(customKey, keyAgain);
   }

   public void testStringToKeyWithDefaultTransformer() {
      CustomKey2 ck2 = new CustomKey2(Integer.MAX_VALUE, Integer.MIN_VALUE, 0);
      String strRep = keyTransformationHandler.keyToString(ck2, 12);
      Object keyAgain = keyTransformationHandler.stringToKey(strRep);
      assertEquals(ck2, keyAgain);
   }

   public void testStringToKeyWithRegisteredTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey3.class, CustomKey3Transformer.class);

      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key, 22);
      Object keyAgain = keyTransformationHandler.stringToKey(string);
      assertEquals(key, keyAgain);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToKeyWithNoAvailableTransformer() {
      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key, 0);
      Object keyAgain = keyTransformationHandler.stringToKey(string);
      assertEquals(key, keyAgain);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testKeyToStringWithExceptionalTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey2.class, ExceptionThrowingTransformer.class);

      CustomKey2 key = new CustomKey2(1, 2, 3);
      keyTransformationHandler.keyToString(key, 1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testKeyToStringWithDefaultTransformerForNonSerializableObject() {
      NonSerializableKey key = new NonSerializableKey("test");
      keyTransformationHandler.keyToString(key, 23);
   }

   public static class ExceptionThrowingTransformer implements Transformer {

      public ExceptionThrowingTransformer() {
         throw new RuntimeException("Shaka Laka Boom Boom");
      }

      @Override
      public Object fromString(String s) {
         return null;
      }

      @Override
      public String toString(Object customType) {
         return null;
      }
   }
}
