package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.persistence.JdbcConfigurationUtil;
import org.infinispan.server.persistence.PersistenceIT;
import org.infinispan.server.persistence.TableManipulation;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class RollingUpgradePersistenceTestIT {

   private static final int NUM_ENTRIES = 100;

   @Test
   public void testRollingUpgradeWithSIFS() throws Throwable {
      String cacheName = "rolling-upgrade";
      int nodeCount = 3;
      String xml = """
            <distributed-cache name="%s">
              <persistence passivation="false">
                <file-store shared="false" />
              </persistence>
            </distributed-cache>
            """.formatted(cacheName);

      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(RollingUpgradePersistenceTestIT.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .jgroupsProtocol("tcp")
            .nodeCount(nodeCount);
      builder.handlers(
            uh -> handleInitializer(uh, cacheName, new StringConfiguration(xml)),
            uh -> assertDataIsCorrect(uh, cacheName)
            );

      RollingUpgradeHandler.performUpgrade(builder.build());
   }

   @ParameterizedTest
   @org.infinispan.server.test.core.tags.Database
   @ArgumentsSource(PersistenceIT.DefaultDatabaseTypes.class)
   public void testRollingUpgradeWithJdbcString(String databaseType) throws Throwable {
      String cacheName = "rolling_upgrade_jdbc";
      int nodeCount = 2;
      DatabaseServerListener listener = new DatabaseServerListener(databaseType);
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(RollingUpgradePersistenceTestIT.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .nodeCount(nodeCount)
            .addArchives(PersistenceIT.getJavaArchive())
            .addMavenArtifacts(PersistenceIT.getJdbcDrivers())
            .addProperty(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
            .jgroupsProtocol("tcp")
            .addListener(listener);

      Consumer<TableManipulation> validateTable = table -> {
         try (table) {
            assertThat(table.countAllRows()).isEqualTo(NUM_ENTRIES);
            for (int i = 0; i < NUM_ENTRIES; i++) {
               try {
                  assertThat(table.getValueByKey("key-" + i)).isNotNull();
               } catch (Exception e) {
                  throw new AssertionError(e);
               }
            }
         }
      };

      builder.handlers(ruh -> {
         Database database = listener.getDatabase(databaseType);
         JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
               .setLockingConfigurations();
         handleInitializer(ruh, cacheName, jdbcUtil.getConfigurationBuilder().build());
         validateTable.accept(new TableManipulation(cacheName, jdbcUtil.getPersistenceConfiguration()));
      }, ruh -> {
         Database database = listener.getDatabase(databaseType);
         JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
               .setLockingConfigurations();
         assertDataIsCorrect(ruh, cacheName);
         validateTable.accept(new TableManipulation(cacheName, jdbcUtil.getPersistenceConfiguration()));
         return true;
      });

      RollingUpgradeHandler.performUpgrade(builder.build());
   }

   private void handleInitializer(RollingUpgradeHandler uh, String cacheName, BasicConfiguration configuration) {
      RemoteCache<String, String> cache = uh.getRemoteCacheManager()
            .administration()
            .getOrCreateCache(cacheName, configuration);

      for (int i = 0; i < NUM_ENTRIES; i++) {
         cache.put("key-" + i, "value-" + i);
      }

      assertThat(cache.size()).isEqualTo(NUM_ENTRIES);
   }

   private boolean assertDataIsCorrect(RollingUpgradeHandler ruh, String cacheName) {
      RemoteCache<String, String> cache = ruh.getRemoteCacheManager().getCache(cacheName);
      assertThat(cache.size()).isEqualTo(NUM_ENTRIES);

      for (int i = 0; i < NUM_ENTRIES; i++) {
         assertThat(cache.get("key-" + i)).isEqualTo("value-" + i);
      }

      return true;
   }
}
