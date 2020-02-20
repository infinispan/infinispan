package org.infinispan.cli.interpreter;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "xsite", testName = "cli.interpreter.SiteStatementTest")
public class SiteStatementTest extends AbstractTwoSitesTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   public SiteStatementTest() {
      implicitBackupCache = true;
   }

   protected GlobalConfigurationBuilder globalConfigurationBuilderForSite(String siteName) {
      GlobalConfigurationBuilder builder = super.globalConfigurationBuilderForSite(siteName);
      configureJmx(builder, getClass().getSimpleName() + "-" + siteName, mBeanServerLookup);
      return builder;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testSiteStatus() throws Exception {
      Interpreter lonInterpreter = interpreter(LON, 0);
      String lonCache = cache(LON, 0).getName();
      String lonSessionId = lonInterpreter.createSessionId(lonCache);
      Interpreter nycInterpreter = interpreter(NYC, 0);
      String nycCache = cache(NYC, 0).getName();
      String nycSessionId = nycInterpreter.createSessionId(nycCache);

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status \"%s\";", NYC), "online");

      assertInterpreterOutput(nycInterpreter, nycSessionId, format("site --status %s.%s;", lonCache, LON), "online");

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --offline %s;", NYC), "ok");

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --online %s;", NYC), "ok");

   }

   @Test(groups = "unstable", description = "ISPN-8202")
   public void testSiteStateTransfer() throws Exception {
      Interpreter lonInterpreter = interpreter(LON, 0);
      String lonCache = cache(LON, 0).getName();
      String lonSessionId = lonInterpreter.createSessionId(lonCache);
      Interpreter nycInterpreter = interpreter(NYC, 0);
      String nycCache = cache(NYC, 0).getName();
      String nycSessionId = nycInterpreter.createSessionId(nycCache);

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --offline %s;", NYC), "ok");
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status %s;", NYC), "offline");
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --push %s;", NYC), "ok");

      assertEventuallyNoStateTransferInReceivingSite(NYC, nycCache, 10, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, lonCache, 10, TimeUnit.SECONDS);

      assertInterpreterOutput(nycInterpreter, nycSessionId, "site --sendingsite;", "null");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --pushstatus;", format("%s=OK", NYC));
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --clearpushstatus;", "ok");
      assertInterpreterOutput(lonInterpreter, lonSessionId, "site --pushstatus;", (String) null);
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --cancelpush %s;", NYC), "ok");
      assertInterpreterOutput(nycInterpreter, nycSessionId, format("site --cancelreceive %s;", LON), "ok");
   }

   public void testSiteWithoutBackups() {
      final String cacheName = "no-backups";
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      configureJmx(global, getClass().getSimpleName(), mBeanServerLookup);
      withCacheManager(new CacheManagerCallable(createCacheManager(global, new ConfigurationBuilder())) {
         @Override
         public void call() {
            ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
            builder.sites().disableBackups(true);
            cm.defineConfiguration(cacheName, builder.build());
            Cache<Object, Object> cache = cm.getCache(cacheName);
            Interpreter interpreter = TestingUtil.extractComponent(cache, Interpreter.class);
            String sessionId = interpreter.createSessionId(cacheName);
            try {
               assertInterpreterError(interpreter, sessionId, "site --status;",
                                      "ISPN019033: The cache '" + cacheName + "' has no backups configured.");
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      });
   }

   public void testContainerOperations() throws Exception {
      Arrays.asList(LON, NYC).forEach(s -> {
         site(s).cacheManagers().forEach(
               cacheManager -> cacheManager.defineConfiguration("another-cache", lonConfigurationBuilder().build()));
         site(s).cacheManagers().forEach(cacheManager -> cacheManager
               .defineConfiguration("another-cache-2", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build()));
         site(s).waitForClusterToForm("another-cache");
         site(s).waitForClusterToForm("another-cache-2");
      });

      Interpreter lonInterpreter = interpreter(LON, 0);
      String lonCache = cache(LON, 0).getName();
      String lonSessionId = lonInterpreter.createSessionId(lonCache);
      String defaultCacheName = site(0).cacheManagers().get(0).getCacheManagerConfiguration().defaultCacheName().get();

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --offlineall %s;", NYC), (output, error) -> {
         assertEquals(null, error);
         String outFormat = "%s: %s";
         if (!output.contains(format(outFormat, defaultCacheName, XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", defaultCacheName, output));
         }
         if (!output.contains(format(outFormat, "another-cache", XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", "another-cache", output));
         }
         if (output.contains("another-cache-2")) {
            fail(format("Cache '%s' should not be present in the output: %s", "another-cache-2", output));
         }
      });
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status %s;", NYC), "offline");
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status \"another-cache\".%s;", NYC), "offline");

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --onlineall %s;", NYC), (output, error) -> {
         assertEquals(null, error);
         String outFormat = "%s: %s";
         if (!output.contains(format(outFormat, defaultCacheName, XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", defaultCacheName, output));
         }
         if (!output.contains(format(outFormat, "another-cache", XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", "another-cache", output));
         }
         if (output.contains("another-cache-2")) {
            fail(format("Cache '%s' should not be present in the output: %s", "another-cache-2", output));
         }
      });
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status %s;", NYC), "online");
      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --status \"another-cache\".%s;", NYC), "online");

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --pushall %s;", NYC), (output, error) -> {
         assertEquals(null, error);
         String outFormat = "%s: %s";
         if (!output.contains(format(outFormat, defaultCacheName, XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", defaultCacheName, output));
         }
         if (!output.contains(format(outFormat, "another-cache", XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", "another-cache", output));
         }
         if (output.contains("another-cache-2")) {
            fail(format("Cache '%s' should not be present in the output: %s", "another-cache-2", output));
         }
      });

      assertInterpreterOutput(lonInterpreter, lonSessionId, format("site --cancelpushall %s;", NYC), (output, error) -> {
         assertEquals(null, error);
         String outFormat = "%s: %s";
         if (!output.contains(format(outFormat, defaultCacheName, XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", defaultCacheName, output));
         }
         if (!output.contains(format(outFormat, "another-cache", XSiteAdminOperations.SUCCESS))) {
            fail(format("Cache '%s' should be present in the output: %s", "another-cache", output));
         }
         if (output.contains("another-cache-2")) {
            fail(format("Cache '%s' should not be present in the output: %s", "another-cache-2", output));
         }
      });

   }

   private void assertInterpreterOutput(Interpreter interpreter, String sessionId, String command, String expected) throws Exception {
      assertInterpreterOutput(interpreter, sessionId, command, (output, error) -> {
         assertEquals(null, error);
         assertEquals(expected, output);
      });
   }

   private void assertInterpreterOutput(Interpreter interpreter, String sessionId, String command, OutputValidator validator) throws Exception {
      Objects.requireNonNull(validator);
      Map<String, String> result = interpreter.execute(sessionId, command);
      validator.validate(result.get(ResultKeys.OUTPUT.toString()), result.get(ResultKeys.ERROR.toString()));
   }

   private void assertInterpreterError(Interpreter interpreter, String sessionId, String command, String expected) throws Exception {
      assertInterpreterOutput(interpreter, sessionId, command, (output, error) -> {
         assertEquals(null, output);
         assertEquals(expected, error);
      });
   }

   private Interpreter interpreter(String site, int cache) {
      return TestingUtil.extractComponent(cache(site, cache), Interpreter.class);
   }

   private void assertEventuallyNoStateTransferInReceivingSite(String siteName, String cacheName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, cacheName, cache -> {
         CommitManager commitManager = extractComponent(cache, CommitManager.class);
         return !commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER) &&
               !commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER) &&
               commitManager.isEmpty();
      }, timeout, unit);
   }

   private void assertEventuallyNoStateTransferInSendingSite(String siteName, String cacheName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, cacheName, cache ->
            extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty() &&
            extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty(), timeout, unit);
   }

   private interface OutputValidator {
      void validate(String output, String error);
   }
}
