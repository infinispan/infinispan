package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class H2TableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(H2TableManager.class, Log.class);

   H2TableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         upsertRowSql = String.format("MERGE INTO %1$s (%2$s, %3$s, %4$s) KEY(%4$s) VALUES(?, ?, ?)", getTableName(),
                                      config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return upsertRowSql;
   }
}
