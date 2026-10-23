package org.infinispan.marshall.protostream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.remoting.RemoteException;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Verifies that untrusted class names supplied during unmarshalling are loaded without triggering their static
 * initializers (CWE-470). The fixtures set a system property from their {@code <clinit>}, so any execution of the
 * initializer is observable. Only class literals ({@code X.class}) may be used to obtain fixture names; active use
 * would initialize the class and defeat the test. Each fixture uses its own unique property name that nothing else in
 * the build references, so results do not depend on which forked JVM instance runs this class or what other tests ran
 * before it.
 */
@Test(groups = "unit", testName = "marshall.protostream.UntrustedClassLoadingTest")
public class UntrustedClassLoadingTest extends AbstractInfinispanTest {

   private static final String CLINIT_PROP_PREFIX = "infinispan.test.untrusted-clinit.";

   public void testGenericThrowableRecreateDoesNotRunStaticInitializer() throws Exception {
      String name = ThrowableGadget.class.getName();
      MarshallableThrowable throwable = new MarshallableThrowable(null, name, "boom", null, null);

      Throwable result = throwable.get();

      // the fixture is not a Throwable and must be rejected without being initialized
      assertInstanceOf(RemoteException.class, result);
      assertNull(System.getProperty(CLINIT_PROP_PREFIX + "throwable"),
            "static initializer of an untrusted class must not run during unmarshalling");
   }

   public void testUnknownImplClassReturnsRemoteException() {
      MarshallableThrowable throwable = new MarshallableThrowable(null, "com.example.DoesNotExist", "boom", null, null);

      Throwable result = throwable.get();

      assertInstanceOf(RemoteException.class, result);
   }

   public void testLambdaUnwrapDoesNotRunStaticInitializer() throws Exception {
      String name = LambdaGadget.class.getName();
      MarshallableLambda lambda = new MarshallableLambda(name, "java.util.function.Supplier", "get",
            "()Ljava/lang/Object;", 0, name, "lambda$test", "(LX;)V", null, (MarshallableArray<Object>) null);

      try {
         // the fixture is not a lambda-capturing class so this fails either way; regardless of how it fails we must
         // observe that loading its definition did not run its static initializer
         lambda.unwrap(getClass().getClassLoader());
      } catch (Throwable expected) {
      }

      assertNull(System.getProperty(CLINIT_PROP_PREFIX + "lambda"),
            "static initializer of an untrusted class must not run during unmarshalling");
   }

   public void testStaticInitializerRunsWhenClassIsInitialized() throws Exception {
      // proves the fixtures really have side-effecting static initializers, i.e. that they would have been triggered
      // had Class.forName been called with initialize=true as before this hardening; deliberately no pre-assertion on
      // the property so a re-run in an instance where the canary is already initialized cannot fail it
      Class<?> canary = Class.forName(Canary.class.getName());

      assertEquals("ran", System.getProperty(CLINIT_PROP_PREFIX + "canary"));
   }

   /** A class that is neither a Throwable nor lambda-capturing, simulating an untrusted gadget on the classpath. */
   public static class ThrowableGadget {
      static {
         System.setProperty(CLINIT_PROP_PREFIX + "throwable", "ran");
      }
   }

   public static class LambdaGadget {
      static {
         System.setProperty(CLINIT_PROP_PREFIX + "lambda", "ran");
      }
   }

   /** Initialized on purpose to prove that the {@code <clinit>} side effects are real. */
   public static class Canary {
      static {
         System.setProperty(CLINIT_PROP_PREFIX + "canary", "ran");
      }
   }
}
