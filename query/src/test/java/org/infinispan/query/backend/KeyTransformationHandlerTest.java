package org.infinispan.query.backend;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Base64;
import java.util.UUID;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
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

   private final UUID randomUUID = Util.threadLocalRandomUUID();

   @BeforeMethod
   public void beforeMethod() {
      keyTransformationHandler = new KeyTransformationHandler(null);
   }

   public void testKeyToStringWithStringAndPrimitives() {
      String s = keyTransformationHandler.keyToString("key");
      assertEquals("S:key", s);

      s = keyTransformationHandler.keyToString(1);
      assertEquals("I:1", s);

      s = keyTransformationHandler.keyToString(true);
      assertEquals("B:true", s);

      s = keyTransformationHandler.keyToString((short) 1);
      assertEquals("X:1", s);

      s = keyTransformationHandler.keyToString((long) 1);
      assertEquals("L:1", s);

      s = keyTransformationHandler.keyToString((byte) 1);
      assertEquals("Y:1", s);

      s = keyTransformationHandler.keyToString((float) 1);
      assertEquals("F:1.0", s);

      s = keyTransformationHandler.keyToString('A');
      assertEquals("C:A", s);

      s = keyTransformationHandler.keyToString(1.0);
      assertEquals("D:1.0", s);

      s = keyTransformationHandler.keyToString(randomUUID);
      assertEquals("U:" + randomUUID, s);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      s = keyTransformationHandler.keyToString(arr);
      assertEquals("A:" + Base64.getEncoder().encodeToString(arr), s);
   }

   public void testStringToKeyWithStringAndPrimitives() {
      Object key = keyTransformationHandler.stringToKey("S:key1");
      assertEquals(String.class, key.getClass());
      assertEquals("key1", key);

      key = keyTransformationHandler.stringToKey("I:2");
      assertEquals(Integer.class, key.getClass());
      assertEquals(2, key);

      key = keyTransformationHandler.stringToKey("Y:3");
      assertEquals(Byte.class, key.getClass());
      assertEquals((byte) 3, key);

      key = keyTransformationHandler.stringToKey("F:4.0");
      assertEquals(Float.class, key.getClass());
      assertEquals(4.0f, key);

      key = keyTransformationHandler.stringToKey("L:5");
      assertEquals(Long.class, key.getClass());
      assertEquals((long) 5, key);

      key = keyTransformationHandler.stringToKey("X:6");
      assertEquals(Short.class, key.getClass());
      assertEquals((short) 6, key);

      key = keyTransformationHandler.stringToKey("B:true");
      assertEquals(Boolean.class, key.getClass());
      assertTrue((Boolean) key);

      key = keyTransformationHandler.stringToKey("D:8.0");
      assertEquals(Double.class, key.getClass());
      assertEquals(8.0, key);

      key = keyTransformationHandler.stringToKey("C:9");
      assertEquals(Character.class, key.getClass());
      assertEquals('9', key);

      key = keyTransformationHandler.stringToKey("U:" + randomUUID);
      assertEquals(UUID.class, key.getClass());
      assertEquals(randomUUID, key);

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
