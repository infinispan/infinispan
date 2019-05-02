package org.infinispan.commons.test;

/**
 * Verifies that the tests are being run with a JVM version which matches the supplied regex
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EnvironmentCheck {

   public static final void checkJVMVersion() {
      String jvmVersionRegex = System.getProperty("infinispan.test.jvm.version.regex");
      if (jvmVersionRegex != null) {
         if (!System.getProperty("java.version").matches(jvmVersionRegex)) {
            String message = String.format("JVM version '%s' does not match '%s'\n", System.getProperty("java.version"), jvmVersionRegex);
            throw new RuntimeException(message);
         }
      }
   }
}
