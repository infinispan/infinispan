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
      String s = keyTransformationHandler.keyToString("key");
      assert s.equals("S:key");

      s = keyTransformationHandler.keyToString(1);
      assert s.equals("I:1");

      s = keyTransformationHandler.keyToString(true);
      assert s.equals("B:true");

      s = keyTransformationHandler.keyToString((short) 1);
      assert s.equals("X:1");

      s = keyTransformationHandler.keyToString((long) 1);
      assert s.equals("L:1");

      s = keyTransformationHandler.keyToString((byte) 1);
      assert s.equals("Y:1");

      s = keyTransformationHandler.keyToString((float) 1);
      assert s.equals("F:1.0");

      s = keyTransformationHandler.keyToString('A');
      assert s.equals("C:A");

      s = keyTransformationHandler.keyToString(1.0);
      assert s.equals("D:1.0");

      s = keyTransformationHandler.keyToString(randomUUID);
      assert s.equals("U:" + randomUUID);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      s = keyTransformationHandler.keyToString(arr);
      assert s.equals("A:" + Base64.getEncoder().encodeToString(arr));
   }

   public void testStringToKeyWithStringAndPrimitives() {
      Object key = keyTransformationHandler.stringToKey("S:key1");
      assert key.getClass().equals(String.class);
      assert key.equals("key1");

      key = keyTransformationHandler.stringToKey("I:2");
      assert key.getClass().equals(Integer.class);
      assert key.equals(2);

      key = keyTransformationHandler.stringToKey("Y:3");
      assert key.getClass().equals(Byte.class);
      assert key.equals((byte) 3);

      key = keyTransformationHandler.stringToKey("F:4.0");
      assert key.getClass().equals(Float.class);
      assert key.equals((float) 4.0);

      key = keyTransformationHandler.stringToKey("L:5");
      assert key.getClass().equals(Long.class);
      assert key.equals((long) 5);

      key = keyTransformationHandler.stringToKey("X:6");
      assert key.getClass().equals(Short.class);
      assert key.equals((short) 6);

      key = keyTransformationHandler.stringToKey("B:true");
      assert key.getClass().equals(Boolean.class);
      assert key.equals(true);

      key = keyTransformationHandler.stringToKey("D:8.0");
      assert key.getClass().equals(Double.class);
      assert key.equals(8.0);

      key = keyTransformationHandler.stringToKey("C:9");
      assert key.getClass().equals(Character.class);
      assert key.equals('9');

      key = keyTransformationHandler.stringToKey("U:" + randomUUID);
      assert key.getClass().equals(UUID.class);
      assert key.equals(randomUUID);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      key = keyTransformationHandler.stringToKey("A:" + Base64.getEncoder().encodeToString(arr));
      assertEquals(arr, (byte[]) key);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToUnknownKey() {
      keyTransformationHandler.stringToKey("Z:someKey");
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToKeyWithInvalidTransformer() {
      keyTransformationHandler.stringToKey("T:org.infinispan.InexistentTransformer:key1");
   }

   public void testStringToKeyWithCustomTransformable() {
      CustomKey customKey = new CustomKey(88, 8800, 12889976);
      String strRep = keyTransformationHandler.keyToString(customKey);
      Object keyAgain = keyTransformationHandler.stringToKey(strRep);
      assertEquals(customKey, keyAgain);
   }

   public void testStringToKeyWithDefaultTransformer() {
      CustomKey2 ck2 = new CustomKey2(Integer.MAX_VALUE, Integer.MIN_VALUE, 0);
      String strRep = keyTransformationHandler.keyToString(ck2);
      Object keyAgain = keyTransformationHandler.stringToKey(strRep);
      assertEquals(ck2, keyAgain);
   }

   public void testStringToKeyWithRegisteredTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey3.class, CustomKey3Transformer.class);

      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key);
      Object keyAgain = keyTransformationHandler.stringToKey(string);
      assertEquals(key, keyAgain);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToKeyWithNoAvailableTransformer() {
      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key);
      Object keyAgain = keyTransformationHandler.stringToKey(string);
      assertEquals(key, keyAgain);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testKeyToStringWithExceptionalTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey2.class, ExceptionThrowingTransformer.class);

      CustomKey2 key = new CustomKey2(1, 2, 3);
      keyTransformationHandler.keyToString(key);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testKeyToStringWithDefaultTransformerForNonSerializableObject() {
      NonSerializableKey key = new NonSerializableKey("test");
      keyTransformationHandler.keyToString(key);
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
