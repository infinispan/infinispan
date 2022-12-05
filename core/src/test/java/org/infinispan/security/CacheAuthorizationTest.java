package org.infinispan.security;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "security.CacheAuthorizationTest")
public class CacheAuthorizationTest extends BaseAuthorizationTest {

   public void testAllCombinations() throws Exception {
      Method[] allMethods = SecureCache.class.getMethods();
      Set<String> methodNames = new HashSet<>();
   collectmethods:
      for (Method m : allMethods) {
         StringBuilder s = new StringBuilder("test");
         String name = m.getName();
         s.append(name.substring(0, 1).toUpperCase());
         s.append(name.substring(1));
         for (Class<?> p : m.getParameterTypes()) {
            Package pkg = p.getPackage();
            if (pkg != null && pkg.getName().startsWith("java.util.function"))
               continue collectmethods; // Skip methods which use interfaces introduced in JDK8
            s.append("_");
            s.append(p.getSimpleName().replaceAll("\\[\\]", "Array"));
         }
         methodNames.add(s.toString());
      }
      final SecureCacheTestDriver driver = new SecureCacheTestDriver();
      final SecureCache<String, String> cache = (SecureCache<String, String>) Security.doAs(ADMIN, () -> {
         Cache<String, String> c = cacheManager.getCache();
         return c;
      });
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

   private void invokeCacheMethod(SecureCacheTestDriver driver, AdvancedCache<String, String> cache, String methodName, Method method, AuthorizationPermission expectedPerm, AuthorizationPermission perm) throws Exception {
      try {
         method.invoke(driver, cache);
         if (!perm.implies(expectedPerm)) {
            throw new Exception(String.format("Expected SecurityException while invoking %s with permission %s",
                  methodName, perm));
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
