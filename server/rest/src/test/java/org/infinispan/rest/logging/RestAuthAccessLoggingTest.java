package org.infinispan.rest.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;

import javax.security.auth.Subject;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.skip.StringLogAppender;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestAccessLoggingTest")
public class RestAuthAccessLoggingTest extends SingleCacheManagerTest {
   public static final String REALM = "ApplicationRealm";
   public static final Subject ADMIN = TestingUtil.makeSubject("ADMIN");
   public static final Subject USER = TestingUtil.makeSubject("USER");

   private StringLogAppender logAppender;
   private String testShortName;
   private RestServerHelper restServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      addSecurity(globalBuilder);
      globalBuilder.defaultCacheName("default");
      return Security.doAs(ADMIN, () -> TestCacheManagerFactory.createCacheManager(globalBuilder, new ConfigurationBuilder()));
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      testShortName = TestResourceTracker.getCurrentTestShortName();
      logAppender = new StringLogAppender("org.infinispan.REST_ACCESS_LOG",
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(RestAccessLoggingTest.LOG_FORMAT).build());
      logAppender.install();
      restServer = new RestServerHelper(cacheManager);
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(ADMIN, USER), REALM);
      restServer.withAuthenticator(basicAuthenticator);
      Security.doAs(ADMIN, () -> restServer.start(TestResourceTracker.getCurrentTestShortName()));
   }

   protected void addSecurity(GlobalConfigurationBuilder globalBuilder) {
      globalBuilder.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(new IdentityRoleMapper())
            .role("ADMIN").description("admin role").permission(AuthorizationPermission.ALL)
            .role("USER").description("user role").permission(AuthorizationPermission.WRITE, AuthorizationPermission.READ, AuthorizationPermission.EXEC, AuthorizationPermission.BULK_READ, AuthorizationPermission.CREATE);
   }

   @Override
   protected void teardown() {
      try {
         logAppender.uninstall();
         restServer.stop();
      } catch (Exception ignored) { }
      super.teardown();
   }

   public void testRestAccessLog() throws Exception {
      try (RestClient client = createRestClient(true)) {
         await(client.cache("default").put("key", "value"));
      }

      try (RestClient client = createRestClient(false)) {
         await(client.cache("default").put("key", "value"));
      }

      restServer.stop();
      String version = System.getProperty("infinispan.brand.name");

      assertThat(logAppender.getLog(0))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+] \"PUT /rest/v2/caches/default/key HTTP/1\\.1\" 401 \\d+ \\d+ \\d+ " + version + "/\\p{Graph}+$");
      assertThat(logAppender.getLog(1))
            .matches("^127\\.0\\.0\\.1 ADMIN \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+] \"PUT /rest/v2/caches/default/key HTTP/1\\.1\" 204 \\d+ \\d+ \\d+ " + version + "/\\p{Graph}+$");

      // Unauthenticated client
      assertThat(logAppender.getLog(2))
            .matches("^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+] \"PUT /rest/v2/caches/default/key HTTP/1\\.1\" 401 \\d+ \\d+ \\d+ " + version + "/\\p{Graph}+$");
   }

   private RestClient createRestClient(boolean auth) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(Protocol.HTTP_11).pingOnCreate(false);
      if (auth)
         builder.security().authentication().enable().username("admin").password("admin");

      return RestClient.forConfiguration(builder.build());
   }
}
