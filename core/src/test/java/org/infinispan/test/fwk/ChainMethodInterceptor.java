package org.infinispan.test.fwk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;
import org.testng.TestNGException;
import org.testng.internal.MethodInstance;

/**
 * This is a workaround for TestNG limitation allowing only single IMethodInterceptor instance.
 * Allows to use multiple {@link TestSelector} annotations in the test class hieararchy.
 *
 * Filters are executed before interceptors, and only on those classes that define them. Filters
 * should not have any side-effect and as these only remove test methods, the order of execution
 * is not important.
 *
 * The interceptors on superclasses will be executed before interceptors on subclasses, but
 * an interceptor is executed even on a class that does not define it (because the interceptor
 * is invoked once for the whole suite).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ChainMethodInterceptor implements IMethodInterceptor {
   private static final Log log = LogFactory.getLog(ChainMethodInterceptor.class);

   @Override
   public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
      try {
         Set<Class<? extends IMethodInterceptor>> interceptorSet = new HashSet<>();
         List<Class<? extends IMethodInterceptor>> interceptorList = new ArrayList<>();
         Set<Class<? extends Predicate<IMethodInstance>>> filters = new HashSet<>();
         for (IMethodInstance method : methods) {
            findInterceptors(method.getInstance().getClass(), interceptorSet, interceptorList, filters);
         }
         if (!filters.isEmpty()) {
            Predicate<IMethodInstance>[] filterInstances = filters.stream().map(clazz -> {
               try {
                  return clazz.newInstance();
               } catch (Exception e) {
                  throw new IllegalStateException("Cannot construct filter", e);
               }
            }).toArray(Predicate[]::new);
            ArrayList<IMethodInstance> filteredMethods = new ArrayList<>(methods.size());
METHODS:
            for (IMethodInstance m : methods) {
               for (Predicate<IMethodInstance> filter : filterInstances) {
                  if (hasFilter(m.getInstance().getClass(), filter.getClass()) && !filter.test(m))
                     continue METHODS;
               }
               filteredMethods.add(m);
            }
            methods = filteredMethods;
         }
         for (Class<? extends IMethodInterceptor> interceptor : interceptorList) {
            methods = interceptor.newInstance().intercept(methods, context);
         }
         return methods;
      } catch (Throwable t) {
         MethodInstance methodInstance =
            FakeTestClass.newFailureMethodInstance(new TestNGException(t), context.getCurrentXmlTest(), context);

         return Collections.singletonList(methodInstance);
      }
   }

   private boolean hasFilter(Class<?> clazz, Class<? extends Predicate> filter) {
      if (clazz == null || clazz == Object.class) return false;
      TestSelector annotation = clazz.getAnnotation(TestSelector.class);
      if (annotation != null) {
         for (Class<? extends Predicate<IMethodInstance>> f : annotation.filters()) {
            if (f == filter) return true;
         }
      }
      return hasFilter(clazz.getSuperclass(), filter);
   }

   private void findInterceptors(Class<?> clazz,
                                 Set<Class<? extends IMethodInterceptor>> interceptorSet,
                                 List<Class<? extends IMethodInterceptor>> interceptorList, Set<Class<? extends Predicate<IMethodInstance>>> filters) {
      if (clazz == null || clazz.equals(Object.class)) return;
      findInterceptors(clazz.getSuperclass(), interceptorSet, interceptorList, filters);
      TestSelector annotation = clazz.getAnnotation(TestSelector.class);
      if (annotation != null) {
         for (Class<? extends IMethodInterceptor> interceptor : annotation.interceptors()) {
            if (interceptorSet.add(interceptor)) {
               interceptorList.add(interceptor);
            }
         }
         for (Class<? extends Predicate<IMethodInstance>> filter : annotation.filters()) {
            filters.add(filter);
         }
      }
   }

}
