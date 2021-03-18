package org.infinispan.test;

import java.lang.reflect.Field;
import java.util.List;

import org.infinispan.commons.util.Features;
import org.infinispan.test.fwk.NamedTestMethod;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.internal.BaseTestMethod;

public class FeaturesListener implements IMethodInterceptor {

   @Override
   public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
      Object instance = methods.get(0).getMethod().getInstance();
      Features features = new Features(instance.getClass().getClassLoader());
      AbstractInfinispanTest.FeatureCondition featureCondition = instance.getClass().getAnnotation(AbstractInfinispanTest.FeatureCondition.class);
      if (featureCondition != null && !features.isAvailable(featureCondition.feature())) {
         for (IMethodInstance methodInstance : methods) {
            methodInstance.getMethod().setMissingGroup(featureCondition.feature() + " is disabled.");
         }
         // the annotation is based on the class
         BaseTestMethod baseTestMethod = getBaseMethod(methods.get(0));
         clearBeforeAfterClassMethods(baseTestMethod.getTestClass());
      }
      return methods;
   }

   private void clearBeforeAfterClassMethods(ITestClass testClass) {
      Class<?> superclass = testClass.getClass().getSuperclass();
      try {
         // normally we start the cache managers on before class methods
         Field field = getField(superclass, "m_beforeClassMethods");
         field.set(testClass, new ITestNGMethod[0]);

         // we track threads for tests that ran
         field = getField(superclass, "m_afterClassMethods");
         field.set(testClass, new ITestNGMethod[0]);
      } catch (IllegalAccessException e) {
         throw new IllegalStateException(e);
      }
   }

   private BaseTestMethod getBaseMethod(IMethodInstance methodInstance) {
      ITestNGMethod testNGMethod = methodInstance.getMethod();
      if (testNGMethod instanceof NamedTestMethod) {
         return getObject(testNGMethod, "method");
      } else if (testNGMethod instanceof BaseTestMethod) {
         return (BaseTestMethod) testNGMethod;
      } else {
         throw new IllegalStateException("Method is not a BaseTestMethod");
      }
   }

   private Field getField(Class<?> clazz, String fieldName) {
      try {
         Field field = clazz.getDeclaredField(fieldName);
         field.setAccessible(true);
         return field;
      } catch (NoSuchFieldException e) {
         throw new IllegalStateException(e);
      }
   }

   private <O> O getObject(Object instance, String fieldName) {
      try {
         Field field = getField(instance.getClass(), fieldName);
         return (O) field.get(instance);
      } catch (IllegalAccessException e) {
         throw new IllegalStateException(e);
      }
   }
}
