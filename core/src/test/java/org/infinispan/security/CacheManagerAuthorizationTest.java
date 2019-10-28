package org.infinispan.security;

import static org.testng.Assert.assertTrue;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.Subject;

import org.infinispan.test.Exceptions;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "security.CacheManagerAuthorizationTest")
public class CacheManagerAuthorizationTest extends BaseAuthorizationTest {

   public void testAdminCombinations() throws Exception {
      List<Runnable> calls = Arrays.asList(
            () -> cacheManager.getGlobalComponentRegistry(),
            () -> cacheManager.getCacheManagerConfiguration());

      for (final AuthorizationPermission perm : AuthorizationPermission.values()) {
         for (Runnable fn : calls) {

            PrivilegedExceptionAction<Boolean> action = () -> {
               fn.run();
               return true;
            };

            // only admin must work
            Subject subject = SUBJECTS.get(perm);
            if (perm.implies(AuthorizationPermission.ADMIN)) {
               assertTrue(Security.doAs(subject, action));
            } else {
               Exceptions.expectException(PrivilegedActionException.class, SecurityException.class,
                     () -> Security.doAs(subject, action));
            }
         }
      }
   }
}
