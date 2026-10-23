package org.infinispan.marshall.protostream.impl.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;

import org.infinispan.commons.marshall.MarshallingException;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.ClassAdapterTest")
public class ClassAdapterTest extends AbstractAdapterTest {

   private static final String GADGET_CLASS = "org.infinispan.marshall.protostream.impl.adapters.ClassAdapterTest$ClinitGadget";
   private static final String CANARY_CLASS = "org.infinispan.marshall.protostream.impl.adapters.ClassAdapterTest$InitializerCanary";

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

   public void testClassIsMarshallable() throws Exception {
      Class<?> original = String.class;
      Class<?> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   public void testUntrustedClassNameDoesNotRunStaticInitializer() throws Exception {
      ClassLoader isolated = isolatedClassLoader(GADGET_CLASS);
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(isolated);

         Class<?> clazz = new ClassAdapter().create(GADGET_CLASS);

         assertEquals(GADGET_CLASS, clazz.getName());
         assertTrue(shouldBeInitialized(clazz),
               "static initializer of an untrusted class must not run during unmarshalling");
      } finally {
         Thread.currentThread().setContextClassLoader(original);
      }
   }

   public void testStaticInitializerRunsWhenClassIsInitialized() throws Exception {
      ClassLoader isolated = isolatedClassLoader(CANARY_CLASS);

      Class<?> canary = Class.forName(CANARY_CLASS, true, isolated);

      assertFalse(shouldBeInitialized(canary),
            "class must be initialized when loaded with initialize=true");
   }

   public void testUnknownClassNameThrows() {
      assertThrows(MarshallingException.class, () -> new ClassAdapter().create("com.example.DoesNotExist"));
   }

   public void testArrayClassIsMarshallable() throws Exception {
      Class<?> original = String[].class;

      Class<?> deserialized = deserialize(original);

      assertEquals(original, deserialized);
   }

   public void testPrimitiveClassNameCannotRoundTrip() {
      // Class.forName cannot load a primitive type name ("int"), so a primitive
      // Class cannot be deserialized back through the ClassAdapter.
      assertThrows(MarshallingException.class, () -> deserialize(int.class));
   }

   public static class ClinitGadget {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();
   }

   public static class InitializerCanary {
      @SuppressWarnings("unused")
      private static final Object MARKER = new Object();
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
