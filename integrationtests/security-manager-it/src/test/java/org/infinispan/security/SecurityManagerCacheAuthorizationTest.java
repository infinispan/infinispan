package org.infinispan.security;

import java.security.Policy;

import org.testng.annotations.Test;

/**
 * SecurityManagerCacheAuthorizationTest.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "security.SecurityManagerCacheAuthorizationTest")
public class SecurityManagerCacheAuthorizationTest extends CacheAuthorizationTest {

   @Override
   public void testAllCombinations() throws Exception {
      Policy.setPolicy(new SurefireTestingPolicy());
      System.setSecurityManager(new SecurityManager());
      try {
         super.testAllCombinations();
      } finally {
         System.setSecurityManager(null);
         Policy.setPolicy(null);
      }
   }

}
