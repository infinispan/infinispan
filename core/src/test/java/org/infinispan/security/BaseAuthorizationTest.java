package org.infinispan.security;

import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

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

public abstract class BaseAuthorizationTest extends SingleCacheManagerTest {

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
            BaseAuthorizationTest.super.teardown();
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
}
