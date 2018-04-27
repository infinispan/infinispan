package org.infinispan.tools.store.migrator;

import java.util.Iterator;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class JdbcStoreReader implements Iterable<MarshalledEntry>, AutoCloseable {

   private final MigratorConfiguration config;
   private final StreamingMarshaller marshaller;
   private final ConnectionFactory connectionFactory;

   JdbcStoreReader(MigratorConfiguration config) {
      this.config = config;
      this.marshaller = config.getMarshaller();

      PooledConnectionFactory connectionFactory = new PooledConnectionFactory();
      connectionFactory.start(config.getConnectionConfig(), JdbcStoreReader.class.getClassLoader());
      this.connectionFactory = connectionFactory;
   }

   @Override
   public void close() throws Exception {
      connectionFactory.stop();
   }

   public Iterator<MarshalledEntry> iterator() {
      switch (config.storeType) {
         case BINARY:
            return new BinaryJdbcIterator(connectionFactory, getTableManager(true), marshaller);
         case STRING:
            return new StringJdbcIterator(connectionFactory, getTableManager(false), marshaller,
                  config.getKey2StringMapper());
         case MIXED:
            return new MixedJdbcIterator(connectionFactory, getTableManager(true), getTableManager(false), marshaller,
                  config.getKey2StringMapper());
         default:
            throw new IllegalArgumentException("Unknown Store Type: " + config.storeType);
      }
   }

   private TableManager getTableManager(boolean binary) {
      TableManipulationConfiguration tableConfig = binary ? config.getBinaryTable() : config.getStringTable();
      TableManager tableManager = TableManagerFactory.getManager(config.getDbMeta(), connectionFactory, tableConfig);
      tableManager.setCacheName(config.cacheName);
      return tableManager;
   }
}
