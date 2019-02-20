package org.infinispan.persistence.jdbc.impl.table;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 */
class DB2TableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(DB2TableManager.class, Log.class);

   DB2TableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, String cacheName) {
      super(connectionFactory, config, metaData, cacheName, LOG);
   }

   @Override
   protected String initInsertRowSql() {
      if (metaData.isSegmentedDisabled()) {
         return String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", tableName,
               config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
      } else {
         return String.format("INSERT INTO %s (%s,%s,%s,%s) VALUES (?,?,?,?)", tableName,
               config.idColumnName(), config.timestampColumnName(), config.dataColumnName());
      }
   }

   @Override
   protected String initUpsertRowSql() {
      if (metaData.isSegmentedDisabled()) {
         return String.format("MERGE INTO %1$s AS t " +
                     "USING (SELECT * FROM TABLE (VALUES (?,?,?))) AS tmp(%4$s, %3$s, %2$s) " +
                     "ON t.%4$s = tmp.%4$s " +
                     "WHEN MATCHED THEN UPDATE SET (t.%2$s, t.%3$s) = (tmp.%2$s, tmp.%3$s) " +
                     "WHEN NOT MATCHED THEN INSERT (t.%4$s, t.%3$s, t.%2$s) VALUES (tmp.%4$s, tmp.%3$s, tmp.%2$s)",
               tableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      } else {
         return String.format("MERGE INTO %1$s AS t " +
                     "USING (SELECT * FROM TABLE (VALUES (?,?,?,?))) AS tmp(%4$s, %3$s, %2$s, %5$s) " +
                     "ON t.%4$s = tmp.%4$s " +
                     "WHEN MATCHED THEN UPDATE SET (t.%2$s, t.%3$s, t.%5$s) = (tmp.%2$s, tmp.%3$s, tmp.%5$s) " +
                     "WHEN NOT MATCHED THEN INSERT (t.%4$s, t.%3$s, t.%2$s, t.%5$s) VALUES (tmp.%4$s, tmp.%3$s, tmp.%2$s, tmp.%5$s)",
               tableName, config.dataColumnName(), config.timestampColumnName(), config.idColumnName(), config.segmentColumnName());
      }
   }

   @Override
   public void prepareUpsertStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      ps.setString(1, key);
      ps.setLong(2, timestamp);
      ps.setBinaryStream(3, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
      if (!metaData.isSegmentedDisabled()) {
         ps.setInt(4, segment);
      }
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException {
      super.prepareUpsertStatement(ps, key, timestamp, segment, byteBuffer);
   }

   @Override
   protected String getDropTimestampSql(String indexName) {
      return String.format("DROP INDEX %s", getIndexName(true, indexName));
   }
}
