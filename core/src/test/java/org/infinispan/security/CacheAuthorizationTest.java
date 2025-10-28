package org.infinispan.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "security.CacheAuthorizationTest")
public class CacheAuthorizationTest extends BaseAuthorizationTest {

   public void testAllCombinations() throws Exception {
      Set<String> methodNames = generateSecureDriverMethodNames();
      final SecureCacheTestDriver driver = new SecureCacheTestDriver();
      final SecureCache<String, String> cache = (SecureCache<String, String>) Security.doAs(ADMIN, () -> {
         Cache<String, String> c = cacheManager.getCache();
         return c;
      });
      assertMethodOverridden(cache);
      for (final String methodName : methodNames) {
         Class<? extends SecureCacheTestDriver> driverClass = driver.getClass();
         try {
            final Method method = driverClass.getMethod(methodName, SecureCache.class);
            TestCachePermission annotation = method.getAnnotation(TestCachePermission.class);
            if (annotation == null) {
               throw new Exception(String.format("Method %s on class %s is missing the TestCachePermission annotation",
                     methodName, driver.getClass().getName()));
            }
            final AuthorizationPermission expectedPerm = annotation.value();
            for (final AuthorizationPermission perm : AuthorizationPermission.values()) {
               if (perm == AuthorizationPermission.NONE)
                  continue;// Skip
               log.debugf("Method %s > %s", methodName, perm.toString());
               if (expectedPerm == AuthorizationPermission.NONE) {
                  try {
                     method.invoke(driver, cache);
                  } catch (SecurityException e) {
                     throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm), e);
                  }
               } else {
                  Security.doAs(SUBJECTS.get(perm),() -> {
                     try {
                        invokeCacheMethod(driver, cache, methodName, method, expectedPerm, perm);
                     } catch (Exception e) {
                        throw new RuntimeException(e);
                     }
                  });
                  invokeCacheMethod(driver, cache.withSubject(SUBJECTS.get(perm)), methodName, method, expectedPerm, perm);
               }
            }
         } catch (NoSuchMethodException e) {
            throw new Exception(
                  String.format(
                        "Class %s needs to declare a method with the following signature: public void %s(SecureCache<String, String> cache) {}\n",
                        driver.getClass().getName(), methodName), e);
         }
      }
   }

   private static Set<String> generateSecureDriverMethodNames() {
      Method[] allMethods = SecureCache.class.getMethods();
      Set<String> methodNames = new HashSet<>();
      for (Method m : allMethods) {
         // We are verifying the methods defined by the SecureCache interface only, not the concrete implementations.
         // The interface only annotates methods as secure when they are the default definition that delegates to another.
         // We'll ensure this is still the case at this moment, and make this break if any changes happen in the future.
         if (m.isAnnotationPresent(SecureMethod.class)) {
            assertThat(m.isDefault())
                  .withFailMessage("Method %s.%s(%s) is deemed secure but is not default", m.getDeclaringClass(), m.getName(), Arrays.toString(m.getParameterTypes()))
                  .isTrue();
            continue;
         }
         StringBuilder s = new StringBuilder("test");
         String name = m.getName();
         s.append(name.substring(0, 1).toUpperCase());
         s.append(name.substring(1));
         for (Class<?> p : m.getParameterTypes()) {
            s.append("_");
            s.append(p.getSimpleName().replaceAll("\\[\\]", "Array"));
         }
         methodNames.add(s.toString());
      }
      return methodNames;
   }

   private void assertMethodOverridden(SecureCache<?, ?> concrete) {
      Collection<Method> methods = allHierarchyMethods(concrete.getClass());
      SoftAssertions assertions = new SoftAssertions();

      for (Method method : methods) {
         assertions.assertThat(isMethodOverridden(concrete.getClass(), method))
               .withFailMessage("Method %s.%s(%s) should be overridden", method.getDeclaringClass(), method.getName(), Arrays.toString(method.getParameterTypes()))
               .isTrue();
      }

      assertions.assertAll();
   }

   private Collection<Method> allHierarchyMethods(Class<?> clazz) {
      if (clazz == null || Object.class.equals(clazz))
         return Collections.emptySet();

      Set<Method> methods = new HashSet<>();
      methods.addAll(Arrays.asList(clazz.getMethods()));

      methods.addAll(allHierarchyMethods(clazz.getSuperclass()));
      return methods.stream()
            .filter(m -> !m.isAnnotationPresent(SecureMethod.class))
            .filter(m -> !m.getDeclaringClass().equals(Object.class))
            .filter(m -> !Modifier.isFinal(m.getModifiers()))
            .filter(m -> !Modifier.isStatic(m.getModifiers()))
            .filter(m -> !Modifier.isPrivate(m.getModifiers()))
            .collect(Collectors.toSet());
   }

   private boolean isMethodOverridden(Class<?> concrete, Method method) {
      try {
         // Use DECLARED method to ensure it is defined in the current class and not in the hierarchy.
         Method concreteMethod = concrete.getDeclaredMethod(method.getName(), method.getParameterTypes());
         return !Modifier.isAbstract(concreteMethod.getModifiers());
      } catch (NoSuchMethodException e) {
         return false;
      }
   }

   private void invokeCacheMethod(SecureCacheTestDriver driver, AdvancedCache<String, String> cache, String methodName, Method method, AuthorizationPermission expectedPerm, AuthorizationPermission perm) throws Exception {
      try {
         method.invoke(driver, cache);
         if (!perm.implies(expectedPerm)) {
            throw new Exception(String.format("Expected SecurityException while invoking %s (%s) with permission %s",
                  methodName, expectedPerm, perm));
         }
      } catch (InvocationTargetException e) {
         Throwable cause = e.getCause();
         if (cause instanceof SecurityException) {
            if (perm.implies(expectedPerm)) {
               throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm), e);
            } else {
               // We were expecting a security exception
            }
         } else throw new Exception("Unexpected non-SecurityException", e);
      }
   }

}
