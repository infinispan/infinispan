package org.infinispan.server.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.TestResourceTracker;
import org.infinispan.testing.skip.StringLogAppender;

public abstract class AbstractAuthAccessLoggingTest extends SingleCacheManagerTest {
   public static final String LOG_FORMAT = "%X{address} %X{user} [%d{dd/MMM/yyyy:HH:mm:ss Z}] \"%X{method} %m %X{protocol}\" %X{status} %X{requestSize} %X{responseSize} %X{duration} %X{h:User-Agent}";
   public static Pattern LOG_PATTERN = Pattern.compile("^(?<IP>\\d+\\.\\d+\\.\\d+\\.\\d+) (?<WHO>\\p{Graph}+) (?<WHEN>\\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d+]) \"(?<METHOD>\\p{Graph}+) (?<PATH>\\p{Graph}+) (?<PROTOCOL>\\p{Graph}+)\" (?<STATUS>\\d+|\\w+|\"[^\"]*\") \\d+ \\d+ \\d+\\s?(?<CLIENT>\\p{Graph}+)?$");
   public static final String REALM = "realm";
   public static final Subject ADMIN = TestingUtil.makeSubject("admin");
   public static final Subject READER = TestingUtil.makeSubject("reader");
   public static final Subject WRITER = TestingUtil.makeSubject("writer");
   protected StringLogAppender logAppender;
   private String testShortName;
   public LinkedHashMap<String, String> USERS;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      globalBuilder.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(new IdentityRoleMapper())
            .role("admin").permission(AuthorizationPermission.ALL)
            .role("reader").permission(AuthorizationPermission.ALL_READ)
            .role("writer").permission(AuthorizationPermission.ALL_WRITE, AuthorizationPermission.ALL_READ);
      globalBuilder.defaultCacheName("default");
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authorization().enable();
      customCacheConfiguration(builder);
      return Security.doAs(ADMIN, () -> TestCacheManagerFactory.createCacheManager(globalBuilder, builder));
   }

   protected void customCacheConfiguration(ConfigurationBuilder builder) {
      // Overridden by other classes.
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      testShortName = TestResourceTracker.getCurrentTestShortName();
      logAppender = new StringLogAppender(logCategory(),
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().withPattern(LOG_FORMAT).build());
      logAppender.install();
      // preserve the order
      USERS = new LinkedHashMap<>();
      USERS.put("", ""); // anonymous
      USERS.put("writer", "writer");
      USERS.put("reader", "reader");
      USERS.put("wrong", "wrong");
   }

   protected abstract String logCategory();

   @Override
   protected void teardown() {
      try {
         logAppender.uninstall();
      } catch (Exception ignored) {
      }
      super.teardown();
   }

   protected Map<String, String> parseAccessLog(int index) {
      String s = logAppender.get(index);
      Matcher matcher = LOG_PATTERN.matcher(s);
      assertTrue(matcher.matches(), s);
      return Map.of(
            "IP", matcher.group("IP"),
            "WHO", matcher.group("WHO"),
            "WHEN", matcher.group("WHEN"),
            "METHOD", matcher.group("METHOD"),
            "PATH", matcher.group("PATH"),
            "PROTOCOL", matcher.group("PROTOCOL"),
            "STATUS", matcher.group("STATUS"),
            "CLIENT", Objects.requireNonNullElse(matcher.group("CLIENT"), "")
      );
   }
}
