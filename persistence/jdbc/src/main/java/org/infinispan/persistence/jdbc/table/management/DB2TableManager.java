package org.infinispan.persistence.jdbc.table.management;

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

   DB2TableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public String getUpsertRowSql() {
      if (upsertRowSql == null) {
         upsertRowSql = String.format("MERGE INTO %1$s AS t " +
                     "USING (SELECT * FROM TABLE (VALUES (?,?,?))) AS tmp(%4$s, %3$s, %2$s) " +
                     "ON t.%4$s = tmp.%4$s " +
                     "WHEN MATCHED THEN UPDATE SET (t.%2$s, t.%3$s) = (tmp.%2$s, tmp.%3$s) " +
                     "WHEN NOT MATCHED THEN INSERT (t.%4$s, t.%3$s, t.%2$s) VALUES (tmp.%4$s, tmp.%3$s, tmp.%2$s)",
               getTableName(), config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return upsertRowSql;
   }

   @Override
   public void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, ByteBuffer byteBuffer) throws SQLException {
      ps.setString(1, key);
      ps.setLong(2, timestamp);
      ps.setBinaryStream(3, new ByteArrayInputStream(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength()), byteBuffer.getLength());
   }
}
