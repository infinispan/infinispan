package org.infinispan.commons.test.skip;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.IAnnotationTransformer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.ITestAnnotation;
import org.testng.annotations.Test;

public class SkipOnOsListener implements IAnnotationTransformer {

   private static final Class[] TESTNG_ANNOTATIONS = new Class[] {
           BeforeSuite.class,
           AfterSuite.class,
           BeforeClass.class,
           AfterClass.class,
           BeforeMethod.class,
           AfterMethod.class,
           BeforeGroups.class,
           AfterGroups.class
   };

   @Override
   public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
      SkipOnOs annotationOnClass = testClass != null ? (SkipOnOs) testClass.getAnnotation(SkipOnOs.class) : null;
      SkipOnOs annotationOnMethod = testMethod != null ? testMethod.getAnnotation(SkipOnOs.class) : null;

      if(annotationOnMethod != null || annotationOnClass != null) {
         Set<SkipOnOs.OS> skipOnOs = new HashSet<>();
         if(annotationOnMethod != null) {
            skipOnOs.addAll(Arrays.asList(annotationOnMethod.value()));
         }
         if(annotationOnClass != null) {
            checkMethodsOverridden(testClass);
            skipOnOs.addAll(Arrays.asList(annotationOnClass.value()));
         }
         if(skipOnOs.contains(SkipOnOsUtils.getOs())) {
            annotation.setEnabled(false);
            String msg = "Skipping " + (testMethod != null ? testMethod.getName() : testClass != null ? testClass.getName() : null) + " on " + skipOnOs;
            annotation.setDescription(msg);
            System.out.println(msg);
         }
      }
   }

   private void checkMethodsOverridden(Class testClass) {
      List<Method> currentTestMethods = Arrays.asList(testClass.getDeclaredMethods());
      while ((testClass = testClass.getSuperclass()) != null)
      {
         for (Method m: testClass.getDeclaredMethods()) {
            if (isTestMethod(m) && !isMethodOverridden(m, currentTestMethods)) {
               System.err.println(String.format("Test method %s#%s not overridden. The test will be executed anyway.",
                       testClass.getSimpleName(), m.getName(), SkipOnOs.class.getSimpleName()));
            }
         }
      }
   }

   private boolean isTestMethod(Method m) {
      return (m.getName().startsWith("test") || m.getAnnotation(Test.class) != null)
              && !isTestNGMetaMethod(m);
   }

   private boolean isTestNGMetaMethod(Method m) {
      List<Class> testNGAnnotations = Arrays.asList(TESTNG_ANNOTATIONS);
      return Arrays.asList(m.getAnnotations()).stream().anyMatch(methodAnnotation ->
         testNGAnnotations.stream().anyMatch(testNGAnnotation ->
            testNGAnnotation.isAssignableFrom(methodAnnotation.getClass())));
   }

   private boolean isMethodOverridden(Method method, List<Method> currentTestMethods) {
      return currentTestMethods.stream().anyMatch(m ->
              m.getName().equals(method.getName()) &&
              m.getReturnType().equals(method.getReturnType())
      );
   }
}
