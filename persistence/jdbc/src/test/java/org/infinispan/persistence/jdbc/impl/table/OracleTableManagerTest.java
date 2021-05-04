package org.infinispan.persistence.jdbc.impl.table;

import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
@Test(groups = "functional", testName = "persistence.jdbc.OracleTableManagerTest")
public class OracleTableManagerTest {

   public void testShortIndexNamesOverlap() {
      DbMetaData dbMetaData = new DbMetaData(DatabaseType.ORACLE, 12, 0, false, false, false);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .tableNamePrefix("TBL")
            .dataColumnName("DTC").dataColumnType("BINARY")
            .idColumnName("IDC").idColumnType("VARCHAR(255)")
            .timestampColumnName("TSC").timestampColumnType("BIGINT")
            .segmentColumnName("SGC").segmentColumnType("INT")
            .dataSource().jndiUrl("a_fake_jdni_url");
      Configuration configuration = builder.build();
      JdbcStringBasedStoreConfiguration storeConfiguration = (JdbcStringBasedStoreConfiguration) configuration.persistence().stores().get(0);
      InitializationContextImpl context = new InitializationContextImpl(null, null, null, Mockito.mock(PersistenceMarshaller.class), null, null, null, null, null, null, null);
      OracleTableManager tableManager = new OracleTableManager(context, null, storeConfiguration.table(), dbMetaData, "ALongishCacheName");
      String segmentIndexName = tableManager.getIndexName(true, "segment_index");
      String timestampIndexName = tableManager.getIndexName(true, "timestamp_index");
      assertFalse(segmentIndexName.equals(timestampIndexName));
   }
}
