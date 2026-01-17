package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.audit.LoggingAuditLogger;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.telemetry.OpenTelemetryService;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryService;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingSecurityTest")
public class TracingSecurityTest extends SingleHotRodServerTest {

   public static final String ADMIN_ROLE = "admin";
   public static final String READER_ROLE = "reader";
   public static final Subject ADMIN = TestingUtil.makeSubject(ADMIN_ROLE);
   public static final Subject READER = TestingUtil.makeSubject(READER_ROLE);

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();
   private final LoggingAuditLogger auditLogger = new LoggingAuditLogger();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      OpenTelemetryService telemetryService = new OpenTelemetryService(InMemoryTelemetryService.instance().openTelemetry());
      auditLogger.setTelemetryService(telemetryService);

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .groupOnlyMapping(false)
            .principalRoleMapper(new IdentityRoleMapper()).auditLogger(auditLogger);
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
      telemetryClient.reset();
      Security.doAs(ADMIN, TracingSecurityTest.super::teardown);
   }

   @Override
   protected void clearContent() {
      Security.doAs(ADMIN, () -> cacheManager.getCache().clear());
   }

   public void testReaderReadAllow() {
      Exceptions.expectException(SecurityException.class, () -> Security.doAs(READER, () -> cacheManager.getCache().put("key", "value")));
      Security.doAs(ADMIN, () -> cacheManager.getCache().put("key", "value"));
      Security.doAs(READER, () -> cacheManager.getCache().get("key"));

      eventually(() -> telemetryClient.finishedSpanItems().toString(),
            () -> {
               List<SpanData> spanItems = telemetryClient.finishedSpanItems();
               if (spanItems.size() < 4) {
                  return false;
               }

               Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(spanItems);
               if (spansByName.get("DENY").isEmpty()) {
                  return false;
               }
               return spansByName.get("ALLOW").size() >= 3;
            }, 10, TimeUnit.SECONDS);
      List<SpanData> spanItems = telemetryClient.finishedSpanItems();

      Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(spanItems);
      assertThat(spansByName.get("DENY")).hasSize(1);
      assertThat(spansByName.get("ALLOW")).hasSize(3);
   }
}
