package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class SybaseTableManager extends AbstractTableManager {
   private static final Log LOG = LogFactory.getLog(SybaseTableManager.class, Log.class);

   SybaseTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   protected String getDropTimestampSql(String indexName) {
      return String.format("DROP INDEX %s.%s", getTableName(), getIndexName(true, indexName));
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         updateRowSql = String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = convert(%s,?)",
               getTableName(), config.dataColumnName(), config.timestampColumnName(),
               config.idColumnName(), config.idColumnType());
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         selectRowSql = String.format("SELECT %s, %s FROM %s WHERE %s = convert(%s,?)",
                                      config.idColumnName(), config.dataColumnName(), getTableName(),
                                      config.idColumnName(), config.idColumnType());
      }
      return selectRowSql;
   }

   @Override
   public String getSelectMultipleRowSql(int numberOfParams) {
      String selectCriteria = config.idColumnName() + " = convert(" + config.idColumnType() + ",?)";
      return getSelectMultipleRowSql(numberOfParams, selectCriteria);
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         selectIdRowSql = String.format("SELECT %s FROM %s WHERE %s = convert(%s,?)",
                                        config.idColumnName(), getTableName(), config.idColumnName(), config.idColumnType());
      }
      return selectIdRowSql;
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         deleteRowSql = String.format("DELETE FROM %s WHERE %s = convert(%s,?)",
                                      getTableName(), config.idColumnName(), config.idColumnType());
      }
      return deleteRowSql;
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         if (metaData.isSegmentedDisabled()) {
            upsertRowSql = String.format("MERGE INTO %1$s AS t " +
                        "USING (SELECT ? %2$s, ? %3$s, ? %4$s) AS tmp " +
                        "ON (t.%4$s = tmp.%4$s) " +
                        "WHEN MATCHED THEN UPDATE SET t.%2$s = tmp.%2$s, t.%3$s = tmp.%3$s " +
                        "WHEN NOT MATCHED THEN INSERT VALUES (tmp.%4$s, tmp.%2$s, tmp.%3$s)",
                  this.getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
         } else {
            upsertRowSql = String.format("MERGE INTO %1$s AS t " +
                        "USING (SELECT ? %2$s, ? %3$s, ? %4$s, ? %5$s) AS tmp " +
                        "ON (t.%4$s = tmp.%4$s) " +
                        "WHEN MATCHED THEN UPDATE SET t.%2$s = tmp.%2$s, t.%3$s = tmp.%3$s " +
                        "WHEN NOT MATCHED THEN INSERT VALUES (tmp.%4$s, tmp.%2$s, tmp.%3$s, tmp.%5$s)",
                  this.getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName(),
                  config.segmentColumnName());
         }
      }
      return upsertRowSql;
   }
}
