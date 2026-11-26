package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.internal.InternalCacheNames.CONFIG_STATE_CACHE_NAME;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.io.File;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.MissingMembersException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Cache definitions created while a cluster recovers from a graceful shutdown.
 * <p>
 * After a graceful shutdown, the first member to return may create cache definitions before the other members return.
 * The tests check that those definitions reach every member that returns afterwards. Each test runs with volatile
 * definitions, which are not saved to the configuration storage, and with permanent definitions, which are.
 * </p>
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "globalstate.CacheDefinitionDuringShutdownRecoveryTest")
public class CacheDefinitionDuringShutdownRecoveryTest extends MultipleCacheManagersTest {
   private static final int CLUSTER_SIZE = 3;
   private static final String CACHE_NAME = "existing-cache";
   private static final String NEW_CACHE_NAME = "new-cache";

   private boolean volatileDefinitions;

   @Override
   public Object[] factory() {
      return new Object[]{
            new CacheDefinitionDuringShutdownRecoveryTest().volatileDefinitions(true),
            new CacheDefinitionDuringShutdownRecoveryTest().volatileDefinitions(false),
      };
   }

   /**
    * Selects whether the test creates its cache definitions with {@link AdminFlag#VOLATILE}.
    *
    * @param volatileDefinitions {@code true} to create volatile definitions, {@code false} for permanent ones.
    * @return this test instance.
    */
   private CacheDefinitionDuringShutdownRecoveryTest volatileDefinitions(boolean volatileDefinitions) {
      this.volatileDefinitions = volatileDefinitions;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"volatile"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{volatileDefinitions};
   }

   @Override
   protected void createCacheManagers() {
      Util.recursiveFileRemove(stateDirectory());
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         startManager(i);
      }
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(stateDirectory());
   }

   public void testNewCacheCreatedDuringRecoveryReachesReturningMembers() {
      Configuration configuration = persistentCacheConfiguration();
      writeBeforeShutdown(configuration);
      restartFirstManager();

      Cache<String, String> newCache = createCache(NEW_CACHE_NAME, configuration);
      newCache.put("during", "recovery");
      assertThat(newCache.get("during")).as("Value written on the lone member").isEqualTo("recovery");

      startRemainingManagers();
      assertCacheDataOnAllMembers(NEW_CACHE_NAME, "during", "recovery");
   }

   public void testRecreatedCacheRecoversDataWhenMembersReturn() {
      Configuration configuration = persistentCacheConfiguration();
      writeBeforeShutdown(configuration);
      restartFirstManager();

      // The cache restores its saved topology and waits for its previous members before serving data.
      Cache<String, String> cache = createCache(CACHE_NAME, configuration);
      assertThat(topologyManager(0).isCacheRecoveringShutdown(CACHE_NAME))
            .as("%s recovering from shutdown on the lone member", CACHE_NAME)
            .isTrue();
      assertThatThrownBy(() -> cache.get("before"))
            .as("Read from %s while its previous members are missing", CACHE_NAME)
            .isInstanceOf(MissingMembersException.class);

      startRemainingManagers();
      assertCacheDataOnAllMembers(CACHE_NAME, "before", "shutdown");
   }

   public void testCacheRecreatedAfterMembersReturnRecoversData() {
      Configuration configuration = persistentCacheConfiguration();
      writeBeforeShutdown(configuration);
      restartFirstManager();
      startRemainingManagers();

      createCache(CACHE_NAME, configuration);
      assertCacheDataOnAllMembers(CACHE_NAME, "before", "shutdown");
   }

   public void testShutdownDoesNotSaveConfigurationTopology() {
      writeBeforeShutdown(persistentCacheConfiguration());
      shutdownCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         File persistentLocation = Paths.get(stateDirectory(), Integer.toString(i)).toFile();
         // Control: the shutdown saved the topology of the user cache, so the missing CONFIG state is not vacuous.
         File[] cacheState = persistentLocation.listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertThat(cacheState).as("State file of %s on node %d", CACHE_NAME, i).hasSize(1);
         File[] configState = persistentLocation.listFiles((dir, name) -> name.equals(CONFIG_STATE_CACHE_NAME + ".state"));
         assertThat(configState).as("State file of %s on node %d", CONFIG_STATE_CACHE_NAME, i).isEmpty();
      }
   }

   private Configuration persistentCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.persistence().addSoftIndexFileStore();
      return builder.build();
   }

   /**
    * Creates the cache {@link #CACHE_NAME} on every member and writes one entry, so the graceful shutdown has a user
    * cache and persisted data to save.
    *
    * @param configuration the configuration of the cache.
    */
   private void writeBeforeShutdown(Configuration configuration) {
      createCache(CACHE_NAME, configuration);
      waitForClusterToForm(CACHE_NAME, CONFIG_STATE_CACHE_NAME);
      cache(0, CACHE_NAME).put("before", "shutdown");
   }

   /**
    * Shuts the whole cluster down gracefully and stops every member.
    */
   private void shutdownCluster() {
      ((DefaultCacheManager) manager(0)).shutdownAllCaches();
      TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();
   }

   /**
    * Shuts the whole cluster down gracefully and starts only the first member again.
    * <p>
    * The returning member knows {@link #CACHE_NAME} only when its definition is permanent. Its
    * {@code org.infinispan.CONFIG} starts with a fresh topology, so it does not wait for the missing members.
    * </p>
    */
   private void restartFirstManager() {
      shutdownCluster();
      startManager(0);

      assertThat(manager(0).getMembers()).as("Members after restarting the first node").hasSize(1);
      assertThat(topologyManager(0).isCacheRecoveringShutdown(CONFIG_STATE_CACHE_NAME))
            .as("%s recovering from shutdown on the lone member", CONFIG_STATE_CACHE_NAME)
            .isFalse();
      assertThat(manager(0).getCacheConfiguration(CACHE_NAME) != null)
            .as("Definition of %s known after restart", CACHE_NAME)
            .isEqualTo(!volatileDefinitions);
   }

   /**
    * Starts the other members again and waits until {@code org.infinispan.CONFIG} has every member and has left the
    * shutdown recovery.
    */
   private void startRemainingManagers() {
      for (int i = 1; i < CLUSTER_SIZE; i++) {
         startManager(i);
      }
      waitForClusterToForm(CONFIG_STATE_CACHE_NAME);
      awaitRecovery(CONFIG_STATE_CACHE_NAME);
   }

   /**
    * Asserts that every member defines and runs the cache, and reads the expected value from it.
    *
    * @param name  the cache name.
    * @param key   the key to read on each member.
    * @param value the value each member must return for the key.
    */
   private void assertCacheDataOnAllMembers(String name, String key, String value) {
      eventually(() -> "Members missing the definition of " + name + ": " + membersWithoutDefinition(name),
            () -> membersWithoutDefinition(name).isEmpty());
      eventually(() -> "Cache " + name + " not running on every member",
            () -> cacheManagers.stream().allMatch(manager -> manager.isRunning(name)));
      waitForClusterToForm(name);
      awaitRecovery(name);
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         Cache<String, String> cache = cache(i, name);
         assertThat(cache.get(key)).as("Value of %s on node %d", key, i).isEqualTo(value);
      }
   }

   /**
    * Lists the members that do not know a cache definition.
    *
    * @param name the cache name.
    * @return the addresses of the members without the definition, empty when every member has it.
    */
   private List<String> membersWithoutDefinition(String name) {
      return cacheManagers.stream()
            .filter(manager -> manager.getCacheConfiguration(name) == null)
            .map(EmbeddedCacheManager::getAddress)
            .map(String::valueOf)
            .toList();
   }

   /**
    * Waits until no member reports the cache as recovering from the graceful shutdown.
    *
    * @param name the cache name.
    */
   private void awaitRecovery(String name) {
      eventually(() -> "Cache " + name + " still recovering from shutdown",
            () -> IntStream.range(0, CLUSTER_SIZE).noneMatch(i -> topologyManager(i).isCacheRecoveringShutdown(name)));
   }

   /**
    * Creates a cache from the first member, volatile or permanent as the test instance selects.
    *
    * @param name          the cache name.
    * @param configuration the cache configuration.
    * @return the cache on the first member.
    */
   private Cache<String, String> createCache(String name, Configuration configuration) {
      EnumSet<AdminFlag> flags = volatileDefinitions ? EnumSet.of(AdminFlag.VOLATILE) : EnumSet.noneOf(AdminFlag.class);
      return manager(0).administration().withFlags(flags).getOrCreateCache(name, configuration);
   }

   private LocalTopologyManager topologyManager(int index) {
      return TestingUtil.extractGlobalComponent(manager(index), LocalTopologyManager.class);
   }

   /**
    * The global state folder of this test instance. Each instance has its own, so the volatile and permanent runs
    * never share configuration storage or topology state.
    *
    * @return the folder holding the global state of every member.
    */
   private String stateDirectory() {
      return tmpDirectory(getClass().getSimpleName(), volatileDefinitions ? "volatile" : "permanent");
   }

   private void startManager(int index) {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable()
            .persistentLocation(Paths.get(stateDirectory(), Integer.toString(index)).toString())
            .configurationStorage(ConfigurationStorage.OVERLAY);
      addClusterEnabledCacheManager(global, null);
   }
}
