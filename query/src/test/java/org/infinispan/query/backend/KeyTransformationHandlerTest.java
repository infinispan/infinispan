package org.infinispan.query.backend;

import java.util.UUID;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Base64;
import org.infinispan.query.Transformer;
import org.infinispan.query.test.CustomKey;
import org.infinispan.query.test.CustomKey2;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.NonSerializableKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is the test class for {@link org.infinispan.query.backend.KeyTransformationHandler}
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */

@Test(groups = "functional", testName = "query.backend.KeyTransformationHandlerTest")
public class KeyTransformationHandlerTest {

   String s = null;
   Object key = null;

   private KeyTransformationHandler keyTransformationHandler;
   private static final UUID randomUUID = UUID.randomUUID();

   private static final Log log = LogFactory.getLog(KeyTransformationHandlerTest.class);


   @BeforeMethod
   public void beforeMethod() {
      keyTransformationHandler = new KeyTransformationHandler();
   }

   public void testKeyToStringWithStringAndPrimitives() {
      s = keyTransformationHandler.keyToString("key");
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
      assert s.equals("U:"+randomUUID);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      s = keyTransformationHandler.keyToString(arr);
      assert s.equals("A:"+ Base64.encodeBytes((arr)));
   }

   public void testStringToKeyWithStringAndPrimitives() {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      key = keyTransformationHandler.stringToKey("S:key1", contextClassLoader);
      assert key.getClass().equals(String.class);
      assert key.equals("key1");

      key = keyTransformationHandler.stringToKey("I:2", contextClassLoader);
      assert key.getClass().equals(Integer.class);
      assert key.equals(2);

      key = keyTransformationHandler.stringToKey("Y:3", contextClassLoader);
      assert key.getClass().equals(Byte.class);
      assert key.equals((byte) 3);

      key = keyTransformationHandler.stringToKey("F:4.0", contextClassLoader);
      assert key.getClass().equals(Float.class);
      assert key.equals((float) 4.0);

      key = keyTransformationHandler.stringToKey("L:5", contextClassLoader);
      assert key.getClass().equals(Long.class);
      assert key.equals((long) 5);

      key = keyTransformationHandler.stringToKey("X:6", contextClassLoader);
      assert key.getClass().equals(Short.class);
      assert key.equals((short) 6);

      key = keyTransformationHandler.stringToKey("B:true", contextClassLoader);
      assert key.getClass().equals(Boolean.class);
      assert key.equals(true);

      key = keyTransformationHandler.stringToKey("D:8.0", contextClassLoader);
      assert key.getClass().equals(Double.class);
      assert key.equals(8.0);

      key = keyTransformationHandler.stringToKey("C:9", contextClassLoader);
      assert key.getClass().equals(Character.class);
      assert key.equals('9');

      key = keyTransformationHandler.stringToKey("U:"+randomUUID.toString(), contextClassLoader);
      assert key.getClass().equals(UUID.class);
      assert key.equals(randomUUID);

      byte[] arr = new byte[]{1, 2, 3, 4, 5, 6};
      key = keyTransformationHandler.stringToKey("A:" + Base64.encodeBytes((arr)), contextClassLoader);
      Assert.assertArrayEquals(arr, (byte[]) key);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToUnknownKey() {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

      key = keyTransformationHandler.stringToKey("Z:someKey", contextClassLoader);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testStringToKeyWithInvalidTransformer() {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

      key = keyTransformationHandler.stringToKey("T:org.infinispan.SomeTransformer:key1", contextClassLoader);
   }

   public void testStringToKeyWithCustomTransformable() {
      CustomKey customKey = new CustomKey(88, 8800, 12889976);
      String strRep = keyTransformationHandler.keyToString(customKey);
      assert customKey.equals(keyTransformationHandler.stringToKey(strRep, Thread.currentThread().getContextClassLoader()));
   }

   public void testStringToKeyWithDefaultTransformer() {
      CustomKey2 ck2 = new CustomKey2(Integer.MAX_VALUE, Integer.MIN_VALUE, 0);
      String strRep = keyTransformationHandler.keyToString(ck2);
      assert ck2.equals(keyTransformationHandler.stringToKey(strRep, Thread.currentThread().getContextClassLoader()));
   }

   public void testStringToKeyWithRegisteredTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey3.class, CustomKey3Transformer.class);

      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key);
      assert key.equals(keyTransformationHandler.stringToKey(string, Thread.currentThread().getContextClassLoader()));
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testStringToKeyWithNoAvailableTransformer() {
      CustomKey3 key = new CustomKey3("str");
      String string = keyTransformationHandler.keyToString(key);
      key.equals(keyTransformationHandler.stringToKey(string, Thread.currentThread().getContextClassLoader()));
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testKeyToStringWithExceptionalTransformer() {
      keyTransformationHandler.registerTransformer(CustomKey2.class, ExceptionThrowingTransformer.class);

      CustomKey2 key = new CustomKey2(1, 2, 3);
      String val = keyTransformationHandler.keyToString(key);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testKeyToStringWithDefaultTransformerForNonSerializableObject() {
      NonSerializableKey key = new NonSerializableKey("test");
      String val = keyTransformationHandler.keyToString(key);
   }

   public class ExceptionThrowingTransformer implements Transformer {
      public ExceptionThrowingTransformer() {
         log.info("Exception Throwing Transformer Constructor");

         //simulating exception
         int a = 4 / 0;
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
