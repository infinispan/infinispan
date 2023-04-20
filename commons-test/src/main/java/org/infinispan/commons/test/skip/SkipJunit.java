package org.infinispan.commons.test.skip;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Use as a {@code @Rule} or {@code @ClassRule} to skip all methods in a class on some OSes.
 */
public class SkipJunit implements TestRule {
   private final OS[] oses;
   private final int jdkMajorVersion;

   public SkipJunit(OS... oses) {
      this.oses = Objects.requireNonNull(oses);
      this.jdkMajorVersion = -1;
   }

   public SkipJunit(int jdkMajorVersion) {
      this.jdkMajorVersion = jdkMajorVersion;
      this.oses = null;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      if (oses != null) {
         OS os = OS.getCurrentOs();
         if (!Arrays.asList(oses).contains(os)) {
            return base;
         } else {
            return new Statement() {
               @Override
               public void evaluate() {
                  throw new AssumptionViolatedException("Ignoring test " + description.getDisplayName() + " on OS " + os);
               }
            };
         }
      } else {
         int version = getJDKVersion();
         if (version >= jdkMajorVersion) {
            return new Statement() {
               @Override
               public void evaluate() {
                  throw new AssumptionViolatedException("Ignoring test " + description.getDisplayName() + " on JDK " + version);
               }
            };
         } else {
            return base;
         }
      }
   }

   /**
    * Use within a {@code @Test} method to skip that method on some OSes. Use in a {@code @BeforeClass} method to skip
    * all methods in a class on some OSes.
    */
   public static void skipOnOS(OS... oses) {
      OS os = OS.getCurrentOs();
      if (Arrays.asList(oses).contains(os))
         throw new AssumptionViolatedException("Skipping test on " + os);
   }

   /**
    * Use within a {@code @Test} method to run this test only on certain OSes. Use in a {@code @BeforeClass} method to
    * run all methods in a class only on some OSes.
    */
   public static void onlyOnOS(OS... oses) {
      OS os = OS.getCurrentOs();
      if (!Arrays.asList(oses).contains(os))
         throw new AssumptionViolatedException("Skipping test on " + os);
   }

   public static void skipSinceJDK(int major) {
      int version = getJDKVersion();
      if (version >= major) {
         throw new AssumptionViolatedException("Skipping test on JDK " + version);
      }
   }

   private static int getJDKVersion() {
      String[] parts = System.getProperty("java.version").replaceAll("[^0-9\\.]", "").split("\\.");
      int version = Integer.parseInt(parts[0]);
      if (version == 1)
         version = Integer.parseInt(parts[1]);
      return version;
   }

   public static void skipCondition(Supplier<Boolean> condition) {
      if (condition.get()) {
         throw new AssumptionViolatedException("Skipping test");
      }
   }
}
