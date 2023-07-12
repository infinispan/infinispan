package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.SecureListenerTest")
public class SecureListenerTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A", "listener");
   static final Subject SUBJECT_B = TestingUtil.makeSubject("B", "listener");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles
            .role("listener").permission(AuthorizationPermission.LISTEN)
            .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("listener").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, () -> {
         try {
            cacheManager = createCacheManager();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         if (cache == null) cache = cacheManager.getCache();
      });
   }

   public void testSecureListenerSubject() {
      registerListener(SUBJECT_A);
      registerListener(SUBJECT_B);
      Security.doAs(ADMIN, () -> cacheManager.getCache().put("key", "value"));
   }

   private void registerListener(final Subject subject) {
      Security.doAs(subject, () -> cacheManager.getCache().addListener(new SubjectVerifyingListener(subject)));
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, () -> SecureListenerTest.super.teardown());
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());
   }

   @Listener
   public static final class SubjectVerifyingListener {
      final Subject subject;

      public SubjectVerifyingListener(Subject subject) {
         this.subject = subject;
      }

      @CacheEntryCreated
      public void handleCreated(CacheEntryCreatedEvent<String, String> event) {
         assertEquals(subject, Security.getSubject());
      }

   }
}
