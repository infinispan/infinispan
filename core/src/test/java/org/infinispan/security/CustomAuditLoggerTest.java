package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

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
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.CustomAuditLoggerTest")
public class CustomAuditLoggerTest extends SingleCacheManagerTest {

   public static final String ADMIN_ROLE = "admin";
   public static final String READER_ROLE = "reader";
   public static final Subject ADMIN = TestingUtil.makeSubject(ADMIN_ROLE);
   public static final Subject READER = TestingUtil.makeSubject(READER_ROLE);

   private static final TestAuditLogger LOGGER = new TestAuditLogger();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new IdentityRoleMapper()).auditLogger(LOGGER);
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

      globalRoles.role(ADMIN_ROLE).permission(AuthorizationPermission.ALL).role(READER_ROLE)
            .permission(AuthorizationPermission.READ);
      authConfig.role(ADMIN_ROLE).role(READER_ROLE);
      return TestCacheManagerFactory.createCacheManager(global, config);
   }

   @Override
   protected void setup() throws Exception {
      Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {

         @Override
         public Void run() throws Exception {
            cacheManager = createCacheManager();
            cache = cacheManager.getCache();
            return null;
         }
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            CustomAuditLoggerTest.super.teardown();
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

   public void testAdminWriteAllow() {
      Security.doAs(ADMIN, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            cacheManager.getCache().put("key", "value");
            return null;
         }

      });

      String actual = LOGGER.getLastRecord();
      String expected = LOGGER.formatLogRecord(AuthorizationPermission.WRITE.toString(),
            AuditResponse.ALLOW.toString(), ADMIN.toString());
      assertEquals(expected, actual);
   }

   public void testReaderReadAllow() {
      Security.doAs(READER, new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            cacheManager.getCache().get("key");
            return null;
         }

      });

      String actual = LOGGER.getLastRecord();
      String expected = LOGGER.formatLogRecord(AuthorizationPermission.READ.toString(), AuditResponse.ALLOW.toString(),
            READER.toString());
      assertEquals(expected, actual);
   }

   public void testReaderWriteDeny() {
      try {
         Security.doAs(READER, new PrivilegedAction<Void>() {

            @Override
            public Void run() {
               cacheManager.getCache().put("key", "value");
               return null;
            }

         });
      } catch (SecurityException ingnored) {
      }

      String actual = LOGGER.getLastRecord();
      String expected = LOGGER.formatLogRecord(AuthorizationPermission.WRITE.toString(), AuditResponse.DENY.toString(),
            READER.toString());
      assertEquals(expected, actual);
   }

   public static class TestAuditLogger implements AuditLogger {

      public static final String logTemplate = "Permission to %s is %s for user %s";
      private String lastLogRecord;

      @Override
      public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission,
            AuditResponse response) {
         lastLogRecord = formatLogRecord(permission.toString(), response.toString(), subject.toString());
      }

      public String getLastRecord() {
         return lastLogRecord;
      }

      public String formatLogRecord(String permission, String response, String subject) {
         return String.format(logTemplate, permission, response, subject);
      }
   }

}
