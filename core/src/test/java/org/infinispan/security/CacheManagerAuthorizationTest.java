package org.infinispan.security;

import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import org.infinispan.commons.test.Exceptions;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "security.CacheManagerAuthorizationTest")
public class CacheManagerAuthorizationTest extends BaseAuthorizationTest {

   @TestCachePermission(AuthorizationPermission.ADMIN)
   Runnable GET_GLOBAL_COMPONENT_REGISTRY = () -> cacheManager.getGlobalComponentRegistry();

   @TestCachePermission(AuthorizationPermission.ADMIN)
   Runnable GET_CACHE_MANAGER_CONFIGURATION = () -> cacheManager.getCacheManagerConfiguration();

   @TestCachePermission(AuthorizationPermission.MONITOR)
   Runnable GET_STATS = () -> cacheManager.getStats();

   public void testCombinations() throws Exception {
      Field[] fields = this.getClass().getDeclaredFields();
      for (Field f : fields) {
         if (f.getType().equals(Runnable.class)) {
            final Runnable fn = (Runnable) f.get(this);
            Supplier<Boolean> action = () -> {
               fn.run();
               return true;
            };
            TestCachePermission p = f.getAnnotation(TestCachePermission.class);
            for (final AuthorizationPermission perm : AuthorizationPermission.values()) {
               Subject subject = SUBJECTS.get(perm);
               if (perm.implies(p.value())) {
                  assertTrue(Security.doAs(subject, action));
               } else {
                  Exceptions.expectException(SecurityException.class,
                        () -> Security.doAs(subject, action));
               }
            }
         }
      }
   }
}
