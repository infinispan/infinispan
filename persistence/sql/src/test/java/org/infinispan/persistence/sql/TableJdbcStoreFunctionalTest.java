package org.infinispan.persistence.sql;

import java.util.Arrays;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.sql.configuration.TableJdbcStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "persistence.jdbc.stringbased.TableJdbcStoreFunctionalTest")
public class TableJdbcStoreFunctionalTest extends AbstractSQLStoreFunctionalTest {
   public TableJdbcStoreFunctionalTest(DatabaseType databaseType, boolean transactionalCache,
         boolean transactionalStore) {
      super(databaseType, transactionalCache, transactionalStore);
   }

   @Factory
   public static Object[] factory() {
      return Arrays.stream(new DatabaseType[]{
            DatabaseType.H2,
//            DatabaseType.POSTGRES,
//            DatabaseType.ORACLE,
//            DatabaseType.MARIA_DB,
//            DatabaseType.DB2,
//            DatabaseType.SQL_SERVER,
            DatabaseType.SQLITE,
//            DatabaseType.SYBASE,
//            DatabaseType.MYSQL
      })
            .flatMap(dt -> Stream.of(
                  new TableJdbcStoreFunctionalTest(dt, true, true),
                  new TableJdbcStoreFunctionalTest(dt, true, false),
                  new TableJdbcStoreFunctionalTest(dt, false, false)
            )).toArray();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      TableJdbcStoreConfigurationBuilder storeBuilder = persistence
            .addStore(TableJdbcStoreConfigurationBuilder.class)
            .transactional(transactionalStore)
            .preload(preload);
      configureCommonConfiguration(storeBuilder);

      storeBuilder.tableName(tableToSearch(cacheName));

      createTable(tableToSearch(cacheName), storeBuilder.getConnectionFactory());

      return persistence;
   }
}
