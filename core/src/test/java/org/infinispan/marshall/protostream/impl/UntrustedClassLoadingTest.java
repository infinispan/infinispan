package org.infinispan.marshall.protostream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.WriteSkewException;
import org.testng.annotations.Test;

// Gadget classes are loaded in isolated classloaders so their <clinit> state is deterministic (CWE-470).
@Test(groups = "unit", testName = "marshall.protostream.UntrustedClassLoadingTest")
public class UntrustedClassLoadingTest extends AbstractInfinispanTest {

   private static final String THROWABLE_GADGET_CLASS = "org.infinispan.marshall.protostream.impl.UntrustedClassLoadingTest$ThrowableGadget";
   private static final String LAMBDA_GADGET_CLASS = "org.infinispan.marshall.protostream.impl.UntrustedClassLoadingTest$LambdaGadget";
   private static final String CANARY_CLASS = "org.infinispan.marshall.protostream.impl.UntrustedClassLoadingTest$Canary";
    private static final String GADGET_EXCEPTION_CLASS = "org.infinispan.marshall.protostream.impl.UntrustedClassLoadingTest$GadgetException";

   private static final Object JDK_UNSAFE;
   private static final Method SHOULD_BE_INITIALIZED;
   static {
      try {
         Class<?> unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
         JDK_UNSAFE = unsafeClass.getMethod("getUnsafe").invoke(null);
         SHOULD_BE_INITIALIZED = unsafeClass.getMethod("shouldBeInitialized", Class.class);
      } catch (Exception e) {
         throw new ExceptionInInitializerError(e);
      }
   }

   public void testGenericThrowableRecreateDoesNotRunStaticInitializer() throws Exception {
      ClassLoader isolated = isolatedClassLoader(THROWABLE_GADGET_CLASS);
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(isolated);

         MarshallableThrowable throwable = new MarshallableThrowable(null, THROWABLE_GADGET_CLASS, "boom", null, null);
         Throwable result = throwable.get();

         assertInstanceOf(RemoteException.class, result);
         Class<?> gadget = isolated.loadClass(THROWABLE_GADGET_CLASS);
         assertTrue(shouldBeInitialized(gadget),
               "static initializer of an untrusted class must not run during unmarshalling");
      } finally {
         Thread.currentThread().setContextClassLoader(original);
      }
   }

   public void testUnknownImplClassReturnsRemoteException() {
      MarshallableThrowable throwable = new MarshallableThrowable(null, "com.example.DoesNotExist", "boom", null, null);

      Throwable result = throwable.get();

      assertInstanceOf(RemoteException.class, result);
   }

   public void testLambdaUnwrapDoesNotRunStaticInitializer() throws Exception {
      ClassLoader isolated = isolatedClassLoader(LAMBDA_GADGET_CLASS);

      MarshallableLambda lambda = new MarshallableLambda(LAMBDA_GADGET_CLASS, "java.util.function.Supplier", "get",
            "()Ljava/lang/Object;", 0, LAMBDA_GADGET_CLASS, "lambda$test", "(LX;)V", "()Ljava/lang/Object;",
            MarshallableArray.create(new Object[0]));
      assertThrows(MarshallingException.class, () -> lambda.unwrap(isolated));

      Class<?> gadget = isolated.loadClass(LAMBDA_GADGET_CLASS);
      assertTrue(shouldBeInitialized(gadget),
            "static initializer of an untrusted class must not run during unmarshalling");
   }

   public void testStaticInitializerRunsWhenClassIsInitialized() throws Exception {
      ClassLoader isolated = isolatedClassLoader(CANARY_CLASS);

      Class<?> canary = Class.forName(CANARY_CLASS, true, isolated);

      assertFalse(shouldBeInitialized(canary),
            "class must be initialized when loaded with initialize=true");
   }

   /**
    * Documents a residual CWE-470 exposure: for a {@code Throwable} subtype the generic
    * recreation path instantiates the exception via a constructor, which necessarily
    * triggers class initialization and therefore runs the static initializer.
    */
   public void testThrowableSubtypeStaticInitializerRunsDuringUnmarshalling() throws Exception {
      ClassLoader isolated = isolatedClassLoader(GADGET_EXCEPTION_CLASS);
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(isolated);

         MarshallableThrowable throwable = new MarshallableThrowable(null, GADGET_EXCEPTION_CLASS, "boom", null, null);
         Throwable result = throwable.get();

         // the exception is instantiated by the isolated classloader, so it is a distinct
         // Class object from the one loaded by the test; compare by name instead of type
         assertInstanceOf(Throwable.class, result);
         assertEquals(GADGET_EXCEPTION_CLASS, result.getClass().getName());
         assertEquals("boom", result.getMessage());

         Class<?> gadget = isolated.loadClass(GADGET_EXCEPTION_CLASS);
         assertFalse(shouldBeInitialized(gadget),
               "KNOWN LIMITATION: the static initializer of a Throwable subtype runs during unmarshalling (residual CWE-470)");
      } finally {
         Thread.currentThread().setContextClassLoader(original);
      }
   }

   public void testSafeThrowableRoundTrip() {
      IllegalStateException original = new IllegalStateException("boom");

      Throwable result = MarshallableThrowable.create(original).get();

      assertInstanceOf(IllegalStateException.class, result);
      assertEquals("boom", result.getMessage());
   }

   public void testThrowableWithCauseRoundTrip() {
      IllegalStateException original = new IllegalStateException("boom", new IllegalArgumentException("cause"));

      Throwable result = MarshallableThrowable.create(original).get();

      assertInstanceOf(IllegalStateException.class, result);
      assertEquals("boom", result.getMessage());
      assertInstanceOf(IllegalArgumentException.class, result.getCause());
      assertEquals("cause", result.getCause().getMessage());
   }

   public void testThrowableWithSuppressedRoundTrip() {
      IllegalStateException original = new IllegalStateException("boom");
      original.addSuppressed(new IllegalArgumentException("suppressed"));

      Throwable result = MarshallableThrowable.create(original).get();

      assertInstanceOf(IllegalStateException.class, result);
      assertEquals("boom", result.getMessage());
      Throwable[] suppressed = result.getSuppressed();
      assertEquals(1, suppressed.length);
      assertInstanceOf(IllegalArgumentException.class, suppressed[0]);
      assertEquals("suppressed", suppressed[0].getMessage());
   }

   public void testWriteSkewExceptionRoundTrip() {
      WriteSkewException original = new WriteSkewException("skew", "key");

      Throwable result = MarshallableThrowable.create(original).get();

      assertInstanceOf(WriteSkewException.class, result);
      assertEquals("skew", result.getMessage());
      assertEquals("key", ((WriteSkewException) result).getKey());
   }

   public void testSafeLambdaRoundTrip() {
      SerializableSupplier<String> original = () -> "hello";

      Object result = MarshallableLambda.create(original).unwrap(getClass().getClassLoader());

      assertInstanceOf(SerializableSupplier.class, result);
      assertEquals("hello", ((SerializableSupplier<?>) result).get());
   }

   // A lambda is only serializable (and thus has a writeReplace method) when its
   // functional interface extends Serializable.
   @FunctionalInterface
   private interface SerializableSupplier<T> extends Supplier<T>, java.io.Serializable {
   }

   public static class ThrowableGadget {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();
   }

   public static class LambdaGadget {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();
   }

   public static class Canary {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();
   }

   // A Throwable subtype: unlike the plain gadgets above, recreateGenericThrowable()
   // instantiates it via a constructor, which necessarily triggers class initialization.
   public static class GadgetException extends Exception {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();

      public GadgetException(String message) {
         super(message);
      }
   }

   private static boolean shouldBeInitialized(Class<?> clazz) {
      try {
         return (boolean) SHOULD_BE_INITIALIZED.invoke(JDK_UNSAFE, clazz);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   // Loads className in a child-first classloader so it is guaranteed uninitialized
   private static ClassLoader isolatedClassLoader(String className) {
      return new ClassLoader(ClassLoader.getPlatformClassLoader()) {
         @Override
         protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.equals(className)) {
               synchronized (getClassLoadingLock(name)) {
                  Class<?> c = findLoadedClass(name);
                  if (c == null) {
                     String resource = name.replace('.', '/') + ".class";
                     try (var is = ClassLoader.getSystemClassLoader().getResourceAsStream(resource)) {
                        if (is == null) throw new ClassNotFoundException(name);
                        byte[] bytes = is.readAllBytes();
                        c = defineClass(name, bytes, 0, bytes.length);
                     } catch (IOException e) {
                        throw new ClassNotFoundException(name, e);
                     }
                  }
                  if (resolve) resolveClass(c);
                  return c;
               }
            }
            return ClassLoader.getSystemClassLoader().loadClass(name);
         }
      };
   }
}
