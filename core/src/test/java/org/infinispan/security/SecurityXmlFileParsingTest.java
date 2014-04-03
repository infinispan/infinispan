package org.infinispan.security;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "security.SecurityXmlFileParsingTest")
public class SecurityXmlFileParsingTest extends AbstractInfinispanTest {
   Subject ADMIN = TestingUtil.makeSubject("admin");

   public void testParseAndConstructUnifiedXmlFile() throws Exception {
      Subject.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/security.xml", true)) {
               @Override
               public void call() {
                  GlobalConfiguration g = cm.getCacheManagerConfiguration();

                  assertTrue(g.security().authorization().enabled());
                  assertEquals(IdentityRoleMapper.class, g.security().authorization().principalRoleMapper().getClass());
                  Map<String, Role> globalRoles = g.security().authorization().roles();
                  assertTrue(globalRoles.containsKey("supervisor"));
                  assertTrue(globalRoles.get("supervisor").getPermissions().containsAll(Arrays.asList(AuthorizationPermission.READ, AuthorizationPermission.WRITE, AuthorizationPermission.EXEC)));

                  Configuration c = cm.getCache("secured").getCacheConfiguration();
                  assertTrue(c.security().authorization().enabled());
                  c.security().authorization().roles().containsAll(Arrays.asList("admin", "reader", "writer"));
               }
            });
            return null;
         }
      });

   }

}
