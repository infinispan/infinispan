package org.infinispan.security;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "security.CacheAuthorizationTest")
public class CacheAuthorizationTest extends SingleCacheManagerTest {
   static final Log log = LogFactory.getLog(CacheAuthorizationTest.class);
   static final Subject ADMIN;
   static final Map<AuthorizationPermission, Subject> SUBJECTS;

   static {
      // Initialize one subject per permission
      SUBJECTS = new HashMap<>(AuthorizationPermission.values().length);
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         SUBJECTS.put(perm, TestingUtil.makeSubject(perm.toString() + "_user", perm.toString()));
      }
      ADMIN = SUBJECTS.get(AuthorizationPermission.ALL);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new IdentityRoleMapper());
      final ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.transaction().lockingMode(LockingMode.PESSIMISTIC);
      config.invocationBatching().enable();
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         globalRoles.role(perm.toString()).permission(perm);
         authConfig.role(perm.toString());
      }
      return Security.doAs(ADMIN, new PrivilegedAction<EmbeddedCacheManager>() {
         @Override
         public EmbeddedCacheManager run() {
            return TestCacheManagerFactory.createCacheManager(global, config);
         }
      });
   }

   @Override
   protected void setup() throws Exception {
      cacheManager = createCacheManager();
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            CacheAuthorizationTest.super.teardown();
            return null;
         }
      });
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            cacheManager.getCache().clear();
            return null;
         }
      });
   }

   public void testAllCombinations() throws Exception {
      Method[] allMethods = SecureCache.class.getMethods();
      Set<String> methodNames = new HashSet<String>();
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
      final SecureCache<String, String> cache = (SecureCache<String, String>) Security.doAs(
            ADMIN, (PrivilegedAction<Cache<String, String>>) () -> cacheManager.getCache());
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
               if (annotation.needsSecurityManager() && System.getSecurityManager() == null) {
                  log.debugf("Method %s (skipped, needs SecurityManager)", methodName);
                  break;
               }
               log.debugf("Method %s > %s", methodName, perm.toString());
               if (expectedPerm == AuthorizationPermission.NONE) {
                  try {
                     method.invoke(driver, cache);
                  } catch (SecurityException e) {
                     throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm.toString() ), e);
                  }
               } else {
                  Security.doAs(SUBJECTS.get(perm), (PrivilegedExceptionAction<Void>) () -> {
                     invokeCacheMethod(driver, cache, methodName, method, expectedPerm, perm);
                     return null;
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
                  methodName, perm.toString()));
         }
      } catch (InvocationTargetException e) {
         Throwable cause = e.getCause();
         if (cause instanceof SecurityException) {
            if (perm.implies(expectedPerm)) {
               throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm.toString() ), e);
            } else {
               // We were expecting a security exception
            }
         } else throw new Exception("Unexpected non-SecurityException", e);
      }
   }

}
