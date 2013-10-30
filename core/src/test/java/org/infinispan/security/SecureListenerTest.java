package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups="functional", testName="security.SecureListenerTest")
public class SecureListenerTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");
   static final Subject SUBJECT_A = TestingUtil.makeSubject("A", "listener");
   static final Subject SUBJECT_B = TestingUtil.makeSubject("B", "listener");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization()
            .principalRoleMapper(new IdentityRoleMapper());
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().enable().authorization();

      globalRoles
         .role("listener").permission(AuthorizationPermission.LISTEN)
         .role("admin").permission(AuthorizationPermission.ALL);
      authConfig.role("listener").role("admin");
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      Subject.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            cacheManager = createCacheManager();
            cache = cacheManager.getCache();
            return null;
         }
      });
   }

   public void testSecureListenerSubject() {
      registerListener(SUBJECT_A);
      registerListener(SUBJECT_B);
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            cacheManager.getCache().put("key", "value");
            return null;
         }

      });
   }

   private void registerListener(final Subject subject) {
      Subject.doAs(subject, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            cacheManager.getCache().addListener(new SubjectVerifyingListener(subject));
            return null;
         }

      });
   }

   @Override
   protected void teardown() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            SecureListenerTest.super.teardown();
            return null;
         }
      });
   }

   @Override
   protected void clearContent() {
      Subject.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            cacheManager.getCache().clear();
            return null;
         }
      });
   }

   @Listener
   public static final class SubjectVerifyingListener {
      final Subject subject;

      public SubjectVerifyingListener(Subject subject) {
         this.subject = subject;
      }

      @CacheEntryCreated
      public void handleCreated(CacheEntryCreatedEvent<String, String> event) {
         assertEquals(subject, Subject.getSubject(AccessController.getContext()));
      }

   }
}
