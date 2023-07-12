package org.infinispan.security;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.RolePermissionNoACLCacheTest")
public class RolePermissionNoACLCacheTest extends RolePermissionTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().securityCacheTimeout(0, TimeUnit.SECONDS).authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
            .role("role1").permission(AuthorizationPermission.EXEC)
            .role("role2").permission(AuthorizationPermission.EXEC)
            .role("role3").permission(AuthorizationPermission.EXEC)
            .role("role4").permission(AuthorizationPermission.EXEC)
            .role("role5").permission(AuthorizationPermission.EXEC)
            .role("role6").permission(AuthorizationPermission.EXEC)
            .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("role1").role("role2").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }
}
