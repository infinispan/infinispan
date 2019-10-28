package org.infinispan.test.fwk;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.testng.IInstanceInfo;
import org.testng.TestNGException;
import org.testng.annotations.Test;

public class TestFrameworkFailure<T> implements IInstanceInfo<T> {
   private static final Set<String> OBJECT_METHODS =
         Arrays.stream(Object.class.getMethods())
               .map(Method::getName)
               .collect(Collectors.toSet());
   private final Throwable t;
   private final Class<T> testClass;

   public TestFrameworkFailure(Class<T> testClass, String format, Object... args) {
      this(testClass, new TestNGException(String.format(format, args)));
   }

   public TestFrameworkFailure(Class<T> testClass, Throwable t) {
      this.testClass = testClass;
      this.t = t;
   }

   // All the groups in TestNGSuiteChecksTest.REQUIRED_GROUPS, so the method is not excluded when running from Maven
   @Test(groups = {"unit", "functional", "xsite", "arquillian", "stress", "profiling", "manual", "unstable"})
   public void fail() throws Throwable {
      throw t;
   }

   @Override
   public T getInstance() {
      return Mockito.mock(testClass, invocation -> {
         Method method = invocation.getMethod();

         if (OBJECT_METHODS.contains(method.getName()))
            return invocation.callRealMethod();

         throw t;
      });
   }

   @Override
   public Class<T> getInstanceClass() {
      return testClass;
   }
}
