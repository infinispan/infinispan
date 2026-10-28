package org.infinispan.marshall.protostream.impl.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.ClassAdapterTest")
public class ClassAdapterTest extends AbstractAdapterTest {

   public void testClassIsMarshallable() throws Exception {
      Class<?> original = String.class;
      Class<?> deserialized = deserialize(original);
      assertEquals(original, deserialized);
   }

   /**
    * Resolving a class name during unmarshalling must not trigger the static initializer of an untrusted class. The
    * fixture sets a system property from its {@code <clinit>} so any execution is observable; only the class literal
    * may be used to obtain its name, as active use would initialize it and defeat the test.
    */
   public void testUntrustedClassNameDoesNotRunStaticInitializer() throws Exception {
      String name = ClinitGadget.class.getName();
      // clear in case test-framework class scanning already initialized the gadget
      System.clearProperty(CLINIT_PROP);

      Class<?> clazz = new ClassAdapter().create(name);

      assertEquals(clazz.getName(), name);
      assertNull(System.getProperty(CLINIT_PROP), "static initializer of an untrusted class must not run during unmarshalling");
   }

   public void testStaticInitializerRunsWhenClassIsInitialized() throws Exception {
      // proves the fixture really has a side-effecting static initializer, i.e. that it would have been triggered by
      // Class.forName with initialize=true as before this hardening; deliberately no pre-assertion on the property so
      // a re-run in an instance where the canary is already initialized cannot fail it
      Class<?> canary = Class.forName(InitializerCanary.class.getName());

      assertEquals("ran", System.getProperty(CANARY_PROP));
   }

   private static final String CLINIT_PROP = "infinispan.test.untrusted-clinit.adapter";
   private static final String CANARY_PROP = "infinispan.test.untrusted-clinit.adapter-canary";

   public static class ClinitGadget {
      static {
         System.setProperty(CLINIT_PROP, "ran");
      }
   }

   /** Initialized on purpose to prove that the {@code <clinit>} side effects are real. */
   public static class InitializerCanary {
      static {
         System.setProperty(CANARY_PROP, "ran");
      }
   }
}
