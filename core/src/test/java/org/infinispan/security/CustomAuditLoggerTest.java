package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import javax.security.auth.Subject;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.mappers.IdentityRoleMapper;
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
            .groupOnlyMapping(false)
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
      Security.doAs(ADMIN, () -> {
         try {
            cacheManager = createCacheManager();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         cache = cacheManager.getCache();
      });
   }

   @Override
   protected void teardown() {
      Security.doAs(ADMIN, () -> CustomAuditLoggerTest.super.teardown());
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());
   }

   public void testAdminWriteAllow() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().put("key", "value"));
      String actual = LOGGER.getLastRecord();
      String expected = LOGGER.formatLogRecord(AuthorizationPermission.WRITE.toString(),
            AuditResponse.ALLOW.toString(), ADMIN.toString());
      assertEquals(expected, actual);
   }

   public void testReaderReadAllow() {
      Security.doAs(READER, () -> cacheManager.getCache().get("key"));
      String actual = LOGGER.getLastRecord();
      String expected = LOGGER.formatLogRecord(AuthorizationPermission.READ.toString(), AuditResponse.ALLOW.toString(),
            READER.toString());
      assertEquals(expected, actual);
   }

   public void testReaderWriteDeny() {
      Exceptions.expectException(SecurityException.class, () -> Security.doAs(READER, () -> cacheManager.getCache().put("key", "value")));

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
         lastLogRecord = formatLogRecord(String.valueOf(permission), String.valueOf(response), String.valueOf(subject));
      }

      public String getLastRecord() {
         return lastLogRecord;
      }

      public String formatLogRecord(String permission, String response, String subject) {
         return String.format(logTemplate, permission, response, subject);
      }
   }

}
