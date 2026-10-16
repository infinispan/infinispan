package org.infinispan.query.startup;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Developer;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a cache manager starts when a user cache reindexes at startup and the blocking thread pool uses
 * platform threads.
 * <p>
 * The blocking thread factory is set explicitly, so the test does not depend on {@code org.infinispan.threads.virtual}
 * or on the JDK 21 classes being on the classpath.
 */
@Test(groups = "functional", testName = "query.startup.ReindexStartupPlatformThreadsTest")
public class ReindexStartupPlatformThreadsTest extends AbstractInfinispanTest {

   private static final int ENTRIES = 10;
   private static final long START_TIMEOUT_SECONDS = 30;
   private static final Subject ADMIN = TestingUtil.makeAllSubjects().get(AuthorizationPermission.ALL);

   private final String dataLocation = Testing.tmpDirectory(ReindexStartupPlatformThreadsTest.class.getSimpleName(), "data");
   private final String indexLocation = Testing.tmpDirectory(ReindexStartupPlatformThreadsTest.class.getSimpleName(), "index");
   private final String indexesLocation = Testing.tmpDirectory(ReindexStartupPlatformThreadsTest.class.getSimpleName(), "indexes");

   private EmbeddedCacheManager cacheManager;
   // When true, authorization is enabled. The cache manager starts privileged, everything else runs as ADMIN.
   private boolean secured;

   public void testLocalCacheReindexAtStartup() throws Exception {
      restartAndVerifyReindex(CacheMode.LOCAL);
   }

   public void testSingleNodeClusteredCacheReindexAtStartup() throws Exception {
      restartAndVerifyReindex(CacheMode.DIST_SYNC);
   }

   public void testSecuredCacheReindexAtStartup() throws Exception {
      secured = true;
      restartAndVerifyReindex(CacheMode.LOCAL);
   }

   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      secured = false;
      removeFiles();
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      asAdmin(() -> {
         TestingUtil.killCacheManagers(cacheManager);
         return null;
      });
      removeFiles();
   }

   private void restartAndVerifyReindex(CacheMode mode) throws Exception {
      // First run: write the data to the store. NONE avoids any reindex on this start.
      cacheManager = asAdmin(() -> createCacheManager(mode, globalConfiguration(mode), cacheConfiguration(mode, IndexStartupMode.NONE), true));
      asAdmin(() -> {
         Cache<String, Developer> cache = cacheManager.getCache();
         for (int i = 0; i < ENTRIES; i++) {
            cache.put("dev" + i, new Developer("nick" + i, "dev" + i + "@redmail.io", "Infinispan developer", i, "Infinispan developer"));
         }
         TestingUtil.killCacheManagers(cacheManager);
         return null;
      });

      // Second run: the data is on disk and the indexes are gone, so the cache reindexes while starting.
      GlobalConfigurationBuilder global = TestCacheManagerFactory.platformBlockingThreads(globalConfiguration(mode));
      cacheManager = createCacheManager(mode, global, cacheConfiguration(mode, IndexStartupMode.REINDEX), false);
      // Start like the server does (SecurityActions.startCacheManager): privileged and without a subject.
      // Without authorization, the privileged flag has no effect.
      Future<Void> start = fork(() -> Security.doPrivileged(() -> {
         cacheManager.start();
         return null;
      }));
      // Bounds the start, so a deadlock fails the test instead of hanging the suite.
      start.get(START_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      Cache<String, Developer> restarted = asAdmin(() -> cacheManager.getCache());
      // The reindex may still be running after the start, so wait until every stored entry is indexed.
      eventually(() -> asAdmin(() -> countDevelopers(restarted)) == ENTRIES);
   }

   private void removeFiles() {
      Util.recursiveFileRemove(dataLocation);
      Util.recursiveFileRemove(indexLocation);
      Util.recursiveFileRemove(indexesLocation);
   }

   /**
    * Runs the action as {@link #ADMIN} when the test is secured, or directly otherwise.
    */
   private <T> T asAdmin(Supplier<T> action) {
      return secured ? Security.doAs(ADMIN, action) : action.get();
   }

   private static EmbeddedCacheManager createCacheManager(CacheMode mode, GlobalConfigurationBuilder global,
                                                          ConfigurationBuilder cfg, boolean start) {
      if (mode.isClustered()) {
         return TestCacheManagerFactory.createClusteredCacheManager(start, global, cfg, new TransportFlags());
      }
      return TestCacheManagerFactory.createCacheManager(global, cfg, start);
   }

   private GlobalConfigurationBuilder globalConfiguration(CacheMode mode) {
      GlobalConfigurationBuilder global = mode.isClustered() ? GlobalConfigurationBuilder.defaultClusteredBuilder() : new GlobalConfigurationBuilder();
      if (secured) {
         // One role per permission, mapped from the principal with the same name.
         GlobalAuthorizationConfigurationBuilder roles = global.security().authorization().enable()
               .principalRoleMapper(new IdentityRoleMapper());
         for (AuthorizationPermission perm : AuthorizationPermission.values()) {
            roles.role(perm.toString()).permission(perm);
         }
      }
      return global;
   }

   /**
    * Persistent data with volatile indexes, so {@link IndexStartupMode#REINDEX} triggers a reindex at startup.
    */
   private ConfigurationBuilder cacheConfiguration(CacheMode mode, IndexStartupMode startupMode) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(mode);
      cfg.indexing()
            .enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .path(indexesLocation)
            .startupMode(startupMode)
            .addIndexedEntity(Developer.class);
      cfg.persistence()
            .addSoftIndexFileStore()
            .dataLocation(dataLocation)
            .indexLocation(indexLocation)
            .preload(true);
      if (secured) {
         AuthorizationConfigurationBuilder authorization = cfg.security().authorization().enable();
         for (AuthorizationPermission perm : AuthorizationPermission.values()) {
            authorization.role(perm.toString());
         }
      }
      return cfg;
   }

   private static long countDevelopers(Cache<String, Developer> cache) {
      return cache.query("from " + Developer.class.getName()).execute().count().value();
   }
}
