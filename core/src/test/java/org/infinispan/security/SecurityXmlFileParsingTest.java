package org.infinispan.security;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "security.SecurityXmlFileParsingTest")
public class SecurityXmlFileParsingTest extends AbstractInfinispanTest {
   Subject ADMIN = TestingUtil.makeSubject("admin", "admin");

   public void testParseAndConstructUnifiedXmlFile() {
      Security.doAs(ADMIN, () -> withCacheManager(new CacheManagerCallable(Exceptions.unchecked(() -> TestCacheManagerFactory.fromXml("configs/security.xml", true))) {
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
      }));
   }

   public void testClusterRoleMapperWithRewriter() {
      Security.doAs(ADMIN, () -> withCacheManager(new CacheManagerCallable(Exceptions.unchecked(() -> TestCacheManagerFactory.fromXml("configs/security-role-mapper-rewriter.xml", true))) {
         @Override
         public void call() {
            GlobalConfiguration g = cm.getCacheManagerConfiguration();
            assertTrue(g.security().authorization().enabled());
            assertEquals(ClusterRoleMapper.class, g.security().authorization().principalRoleMapper().getClass());
            ClusterRoleMapper mapper = (ClusterRoleMapper) g.security().authorization().principalRoleMapper();
            mapper.grant("supervisor", "tristan");
            Set<String> roles = mapper.principalToRoles(new TestingUtil.TestPrincipal("cn=tristan,ou=developers,dc=infinispan,dc=org"));
            assertEquals(1, roles.size());
            assertTrue(roles.contains("supervisor"));
         }
      }));
   }

}
