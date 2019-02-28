package org.infinispan.commons.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * This test checks for thread leaks.
 *
 * The check must be in a configuration method, because TestNG doesn't report listener exceptions properly.
 *
 * @since 10.0
 * @author Dan Berindei
 */
@Test(groups = {"unit", "smoke"}, testName = "test.fwk.TestNGSuiteChecksTest")
public class TestNGSuiteChecksTest {
   private static final Set<String> REQUIRED_GROUPS = new HashSet<>(
      Arrays.asList("unit", "functional", "xsite", "arquillian", "stress", "profiling", "manual", "unstable"));
   private static final Set<String> ALLOWED_GROUPS = new HashSet<>(
      Arrays.asList("unit", "functional", "xsite", "arquillian", "stress", "profiling", "manual", "unstable",
                    "smoke", "java10", "transaction"));

   @BeforeSuite(alwaysRun = true)
   public void beforeSuite(ITestContext context) {
      List<String> errors = new ArrayList<>();
      Set<Class> classes = new HashSet<>();
      checkAnnotations(errors, classes, context.getSuite().getExcludedMethods());
      checkAnnotations(errors, classes, context.getSuite().getAllMethods());
      if (!errors.isEmpty()) {
         throw new AssertionError(String.join("\n", errors));
      }
   }

   @AfterSuite(alwaysRun = true)
   public void afterSuite() {
      ThreadLeakChecker.checkForLeaks(TestNGSuiteChecksTest.class.getName());
   }

   private void checkAnnotations(List<String> errors, Set<Class> classes, Collection<ITestNGMethod> methods) {
      for (ITestNGMethod m : methods) {
         checkMethodAnnotations(errors, m);
         checkClassAnnotations(errors, classes, m);
      }
   }

   private void checkMethodAnnotations(List<String> errors, ITestNGMethod m) {
      if (!m.getEnabled())
         return;

      boolean hasRequiredGroup = false;
      for (String g : m.getGroups()) {
         if (!ALLOWED_GROUPS.contains(g)) {
            errors.add(
               "Method " + m.getConstructorOrMethod() +
               " and/or its class has a @Test annotation with the wrong group: " +
               g + ".\nAllowed groups are " + ALLOWED_GROUPS);
            return;
         }
         if (REQUIRED_GROUPS.contains(g)) {
            hasRequiredGroup = true;
         }
      }
      if (!hasRequiredGroup) {
         errors.add("Method " + m.getConstructorOrMethod() + " and/or its class don't have any required group." +
                    "\nRequired groups are " + REQUIRED_GROUPS);
         return;
      }

      // Some stress tests extend a functional test and change some parameters (e.g. number of operations)
      // If the author forgets to override the test methods, they inherit the group from the base class
      Class<?> testClass = m.getRealClass();
      Class<?> declaringClass = m.getConstructorOrMethod().getDeclaringClass();
      Test annotation = testClass.getAnnotation(Test.class);
      if (testClass != declaringClass) {
         if (!Arrays.equals(annotation.groups(), m.getGroups())) {
            errors.add("Method " + m.getConstructorOrMethod() + " was inherited from class " + declaringClass +
                       " with groups " + Arrays.toString(m.getGroups()) +
                       ", but the test class has groups " + Arrays.toString(annotation.groups()));
         }
      }
   }

   private void checkClassAnnotations(List<String> errors, Set<Class> processedClasses, ITestNGMethod m) {
      // Class of the test instance
      Class<?> testClass = m.getTestClass().getRealClass();

      // Check that the test instance's class matches the XmlTest's support class
      // They can be different if a test inherits a @Factory method and doesn't override it
      // Ignore multiple classes in the same XmlTest, which can happen when running from the IDE
      // or when the testName is incorrect (handled below)
      if (m.getXmlTest().getXmlClasses().size() == 1) {
         Class xmlClass = m.getXmlTest().getXmlClasses().get(0).getSupportClass();
         if (xmlClass != testClass && processedClasses.add(xmlClass)) {
            errors.add("Class " + xmlClass.getName() + " must override the @Factory method from base class " +
                       testClass.getName());
            return;
         }
      }

      if (!processedClasses.add(testClass))
         return;

      // Check that the testName in the @Test annotation is set and matches the test class
      // All tests with the same testName or without a testName run in the same XmlTest
      // which means they also run in the same thread, not in parallel
      Test annotation = testClass.getAnnotation(Test.class);
      if (annotation == null || annotation.testName().isEmpty()) {
         errors.add("Class " + testClass.getName() + " does not have a testName");
         return;
      }
      if (!annotation.testName().contains(testClass.getSimpleName())) {
         errors.add("Class " + testClass.getName() + " has an invalid testName: " + annotation.testName());
      }
   }
}
