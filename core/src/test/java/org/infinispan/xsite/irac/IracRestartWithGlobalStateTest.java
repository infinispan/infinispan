package org.infinispan.xsite.irac;

import static java.lang.String.format;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.BEFORE;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.irac.DefaultIracVersionGenerator;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tests the state transfer and checks the versions are kept consistent when a site restart. Global state is available
 * and versions should be stored there to survive the restart.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracRestartWithGlobalStateTest")
public class IracRestartWithGlobalStateTest extends AbstractMultipleSitesTest {

   private static final int NUM_KEYS = 100;
   private final boolean persistent;

   public IracRestartWithGlobalStateTest(boolean persistent) {
      this.persistent = persistent;
   }

   @Factory
   public static Object[] defaultFactory() {
      return new Object[]{
            new IracRestartWithGlobalStateTest(false),
            new IracRestartWithGlobalStateTest(true)
      };
   }

   private static void forEachKeyValue(Method method, String prefix, BiConsumer<String, String> keyValueConsumer) {
      for (int i = 0; i < NUM_KEYS; ++i) {
         keyValueConsumer.accept(k(method, i), v(method, prefix, i));
      }
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() {
      Util.recursiveFileRemove(tmpDirectory(getClass()));
      super.createBeforeClass();
   }

   public void testRestart(Method method) {
      doTest(method, false);
   }

   public void testRestartReverse(Method method) {
      doTest(method, true);
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{null};
   }

   @Override
   protected Object[] parameterValues() {
      return new String[]{persistent ? "PERSISTENT" : "VOLATILE"};
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      if (siteIndex == 0) {
         builder.sites().addBackup()
               .site(siteName(1))
               .strategy(BackupConfiguration.BackupStrategy.ASYNC);
         if (persistent) {
            builder.persistence().addSoftIndexFileStore();
         }
      } else {
         builder.sites().addBackup()
               .site(siteName(0))
               .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      }
      return builder;
   }

   @Override
   protected void decorateGlobalConfiguration(GlobalConfigurationBuilder builder, int siteIndex, int nodeIndex) {
      String stateDirectory = tmpDirectory(getClass().getSimpleName(), "site_" + siteIndex, "node_" + nodeIndex);
      builder.globalState().enable().persistentLocation(stateDirectory);
   }

   private void doTest(Method method, boolean reverse) {
      forEachKeyValue(method, "initial", (k, v) -> cache(0, 0).put(k, v));
      forEachKeyValue(method, "initial", this::eventuallyAssertData);


      Map<Integer, IracEntryVersion> versionsBefore = snapshotPrimaryVersions();
      Map<String, IracEntryVersion> entryVersionsBefore = snapshotKeyVersions(method, 0);
      assertVersions(entryVersionsBefore, snapshotKeyVersions(method, 1), EQUAL);

      log.debug("Stopping site_0");
      stopSite(0);

      log.debug("Starting site_0");
      restartSite(0);

      Map<Integer, IracEntryVersion> versionsAfter = snapshotPrimaryVersions();
      assertVersions(versionsBefore, versionsAfter, EQUAL);

      forEachKeyValue(method, "final", (k, v) -> cache(reverse ? 1 : 0, 0).put(k, v));
      forEachKeyValue(method, "final", this::eventuallyAssertData);

      assertVersions(entryVersionsBefore, snapshotKeyVersions(method, 0), BEFORE);
      assertVersions(entryVersionsBefore, snapshotKeyVersions(method, 1), BEFORE);
   }

   private Map<Integer, IracEntryVersion> snapshotPrimaryVersions() {
      Map<Integer, IracEntryVersion> versions = new HashMap<>(256);
      for (Cache<?, ?> cache : caches(0)) {
         DefaultIracVersionGenerator vGenerator = generator(cache);
         LocalizedCacheTopology topology = extractCacheTopology(cache);
         Map<Integer, IracEntryVersion> cacheVersions = vGenerator.peek();
         log.tracef("Taking snapshot from %s (%s entries): %s", addressOf(cache), cacheVersions.size(), cacheVersions);
         cacheVersions.forEach((segment, version) -> {
            if (topology.getSegmentDistribution(segment).isPrimary()) {
               IracEntryVersion v = versions.putIfAbsent(segment, version);
               assertNull(v);
            }
         });
         log.tracef("Global versions after %s (%s entries): %s", addressOf(cache), versions.size(), versions);
      }
      return versions;
   }

   private Map<String, IracEntryVersion> snapshotKeyVersions(Method method, int siteIndex) {
      Map<String, IracEntryVersion> versions = new HashMap<>(256);
      for (Cache<String, String> cache : this.<String, String>caches(siteIndex)) {
         LocalizedCacheTopology topology = extractCacheTopology(cache);
         //noinspection unchecked
         InternalDataContainer<String, String> dataContainer = extractComponent(cache, InternalDataContainer.class);
         for (int i = 0; i < NUM_KEYS; ++i) {
            String key = k(method, i);
            DistributionInfo distributionInfo = topology.getDistribution(key);
            if (distributionInfo.isPrimary()) {
               IracEntryVersion version = dataContainer.peek(distributionInfo.segmentId(), key).getInternalMetadata()
                     .iracMetadata().getVersion();
               AssertJUnit.assertNotNull(version);
               versions.put(key, version);
            }
         }
      }
      return versions;
   }

   private static DefaultIracVersionGenerator generator(Cache<?, ?> cache) {
      return (DefaultIracVersionGenerator) extractComponent(cache, IracVersionGenerator.class);
   }

   private static <K> void assertVersions(Map<K, IracEntryVersion> v1, Map<K, IracEntryVersion> v2,
                                          InequalVersionComparisonResult expected) {
      assertEquals(v1.size(), v2.size());
      Iterator<K> iterator = Stream.concat(v1.keySet().stream(), v2.keySet().stream())
            .distinct()
            .iterator();
      while (iterator.hasNext()) {
         K key = iterator.next();
         IracEntryVersion version1 = v1.get(key);
         IracEntryVersion version2 = v2.get(key);
         assertNotNull(format("'%s' version is null for Map 1", key), version1);
         assertNotNull(format("'%s' version is null for Map 2", key), version2);
         InequalVersionComparisonResult result = version1.compareTo(version2);
         assertEquals(format("'%s' version mismatch: %s and %s", key, version1, version2), expected, result);
      }

   }

   private void eventuallyAssertData(String key, String value) {
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(value, cache.get(key)));
   }
}
