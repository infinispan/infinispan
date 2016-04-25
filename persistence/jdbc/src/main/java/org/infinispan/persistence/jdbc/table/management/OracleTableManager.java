package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author Ryan Emerson
 */
class OracleTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(OracleTableManager.class, Log.class);

   OracleTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema() == null ? metaData.getUserName() : tableName.getSchema();
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[] {"TABLE"});
         return rs.next();
      } catch (SQLException e) {
         if (LOG.isTraceEnabled())
            LOG.tracef(e, "SQLException occurs while checking the table %s", tableName);
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
      }
   }
}