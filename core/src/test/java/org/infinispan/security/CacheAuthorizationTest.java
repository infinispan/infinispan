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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.SingleCacheManagerTest")
public class CacheAuthorizationTest extends SingleCacheManagerTest {
   Map<AuthorizationPermission, Subject> subjects;

   @BeforeClass
   void initializeSubjects() {      // Initialize one subject per permission
      subjects = new HashMap<AuthorizationPermission, Subject>(AuthorizationPermission.values().length);
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         subjects.put(perm, TestingUtil.makeSubject(perm.toString() + "_user", perm.toString()));
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization()
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.transaction().lockingMode(LockingMode.PESSIMISTIC);
      config.invocationBatching().enable();
      AuthorizationConfigurationBuilder authConfig = config.security().enable().authorization();

      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         globalRoles.role(perm.toString()).permission(perm);
         authConfig.role(perm.toString());
      }
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      cacheManager = createCacheManager();
   }

   @Override
   protected void teardown() {
      Subject.doAs(subjects.get(AuthorizationPermission.ALL), new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            CacheAuthorizationTest.super.teardown();
            return null;
         }
      });
   }

   @Override
   protected void clearContent() {
      Subject.doAs(subjects.get(AuthorizationPermission.ALL), new PrivilegedAction<Void>() {
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
      for (Method m : allMethods) {
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
      final SecureCacheTestDriver driver = new SecureCacheTestDriver();
      final SecureCache<String, String> cache = (SecureCache<String, String>) Subject.doAs(
            subjects.get(AuthorizationPermission.ALL), new PrivilegedAction<Cache<String, String>>() {
               @Override
               public Cache<String, String> run() {
                  return cacheManager.getCache();
               }
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
            System.out.printf("Method: %s ", methodName);
            for (final AuthorizationPermission perm : AuthorizationPermission.values()) {
               if (perm == AuthorizationPermission.NONE)
                  continue;// Skip
               System.out.printf(" %s", perm.toString());
               if (expectedPerm == AuthorizationPermission.NONE) {
                  try {
                     method.invoke(driver, cache);
                  } catch (SecurityException e) {
                     throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm.toString() ), e);
                  }
               } else {
                  Subject.doAs(subjects.get(perm), new PrivilegedExceptionAction<Void>() {

                     @Override
                     public Void run() throws Exception {
                        try {
                           method.invoke(driver, cache);
                           if (perm != expectedPerm && perm != AuthorizationPermission.ALL) {
                              throw new Exception(String.format("Expected SecurityException while invoking %s with permission %s",
                                    methodName, perm.toString()));
                           }
                           return null;
                        } catch (InvocationTargetException e) {
                           Throwable cause = e.getCause();
                           if (cause instanceof SecurityException) {
                              if (perm == expectedPerm && perm != AuthorizationPermission.ALL) {
                                 throw new Exception(String.format("Unexpected SecurityException while invoking %s with permission %s", methodName, perm.toString() ), e);
                              } else {
                                 // We were expecting a security exception
                                 return null;
                              }
                           } else throw new Exception("Unexpected non-SecurityException", e);
                        }
                     }
                  });
               }
            }
            System.out.println();

         } catch (NoSuchMethodException e) {
            throw new Exception(
                  String.format(
                        "Class %s needs to declare a method with the following signature: void %s(SecureCache<String, String> cache) {}\n",
                        driver.getClass().getName(), methodName), e);
         }
      }
   }

}
