package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * @author Ryan Emerson
 */
public abstract class AbstractTableManager implements TableManager {

   private final Log log;
   protected final ConnectionFactory connectionFactory;
   protected final TableManipulationConfiguration config;

   protected String identifierQuoteString = "\"";
   protected String cacheName;
   protected DbMetaData metaData;
   protected TableName tableName;

   protected String insertRowSql;
   protected String updateRowSql;
   protected String selectRowSql;
   protected String selectIdRowSql;
   protected String deleteRowSql;
   protected String loadAllRowsSql;
   protected String countRowsSql;
   protected String loadAllNonExpiredRowsSql;
   protected String deleteAllRows;
   protected String selectExpiredRowsSql;
   protected String deleteExpiredRowsSql;

   AbstractTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData, Log log) {
      this.connectionFactory = connectionFactory;
      this.config = config;
      this.metaData = metaData;
      this.log = log;
   }

   @Override
   public void start() throws PersistenceException {
      if (config.createOnStart()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            if (!tableExists(conn)) {
               createTable(conn);
            }
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void stop() throws PersistenceException {
      if (config.dropOnExit()) {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            dropTable(conn);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }
   }

   @Override
   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
      tableName = null;
   }

   public boolean tableExists(Connection connection) throws PersistenceException {
      return tableExists(connection, getTableName());
   }

   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      Objects.requireNonNull(tableName, "table name is mandatory");
      ResultSet rs = null;
      try {
         // we need to make sure, that (even if the user has extended permissions) only the tables in current schema are checked
         // explicit set of the schema to the current user one to make sure only tables of the current users are requested
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema();
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[]{"TABLE"});
         return rs.next();
      } catch (SQLException e) {
         if (log.isTraceEnabled())
            log.tracef(e, "SQLException occurs while checking the table %s", tableName);
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
      }
   }

   public void createTable(Connection conn) throws PersistenceException {
      if (cacheName == null || cacheName.trim().length() == 0)
         throw new PersistenceException("cacheName needed in order to create table");

      String ddl = String.format("CREATE TABLE %1$s (%2$s %3$s NOT NULL, %4$s %5$s, %6$s %7$s, PRIMARY KEY (%2$s))",
                                 getTableName(), config.idColumnName(), config.idColumnType(), config.dataColumnName(),
                                 config.dataColumnType(), config.timestampColumnName(), config.timestampColumnType());

      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", ddl);
      }
      executeUpdateSql(conn, ddl);
   }

   public void executeUpdateSql(Connection conn, String sql) throws PersistenceException {
      Statement statement = null;
      try {
         statement = conn.createStatement();
         statement.executeUpdate(sql);
      } catch (SQLException e) {
         log.errorCreatingTable(sql, e);
         throw new PersistenceException(e);
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   public void dropTable(Connection conn) throws PersistenceException {
      String dropTableDdl = "DROP TABLE " + getTableName();
      String clearTable = "DELETE FROM " + getTableName();
      executeUpdateSql(conn, clearTable);
      if (log.isTraceEnabled()) {
         log.tracef("Dropping table with following DDL '%s'", dropTableDdl);
      }
      executeUpdateSql(conn, dropTableDdl);
   }

   public int getFetchSize() {
      return config.fetchSize();
   }

   public int getBatchSize() {
      return config.batchSize();
   }

   public String getIdentifierQuoteString() {
      return identifierQuoteString;
   }

   public TableName getTableName() {
      if (tableName == null) {
         tableName = new TableName(identifierQuoteString, config.tableNamePrefix(), cacheName);
      }
      return tableName;
   }

   @Override
   public String getInsertRowSql() {
      if (insertRowSql == null) {
         insertRowSql = String.format("INSERT INTO %s (%s,%s,%s) VALUES (?,?,?)", getTableName(),
                                      config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return insertRowSql;
   }

   @Override
   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         updateRowSql = String.format("UPDATE %s SET %s = ? , %s = ? WHERE %s = ?", getTableName(),
                                      config.dataColumnName(), config.timestampColumnName(), config.idColumnName());
      }
      return updateRowSql;
   }

   @Override
   public String getSelectRowSql() {
      if (selectRowSql == null) {
         selectRowSql = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
                                      config.idColumnName(), config.dataColumnName(), getTableName(), config.idColumnName());
      }
      return selectRowSql;
   }

   @Override
   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         selectIdRowSql = String.format("SELECT %s FROM %s WHERE %s = ?", config.idColumnName(), getTableName(), config.idColumnName());
      }
      return selectIdRowSql;
   }

   @Override
   public String getCountRowsSql() {
      if (countRowsSql == null) {
         countRowsSql = "SELECT COUNT(*) FROM " + getTableName();
      }
      return countRowsSql;
   }

   @Override
   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         deleteRowSql = String.format("DELETE FROM %s WHERE %s = ?", getTableName(), config.idColumnName());
      }
      return deleteRowSql;
   }

   @Override
   public String getLoadNonExpiredAllRowsSql() {
      if (loadAllNonExpiredRowsSql == null) {
         loadAllNonExpiredRowsSql = String.format("SELECT %1$s, %2$s, %3$s FROM %4$s WHERE %3$s > ? OR %3$s < 0",
                                                  config.dataColumnName(), config.idColumnName(),
                                                  config.timestampColumnName(), getTableName());
      }
      return loadAllNonExpiredRowsSql;
   }

   @Override
   public String getLoadAllRowsSql() {
      if (loadAllRowsSql == null) {
         loadAllRowsSql = String.format("SELECT %s, %s FROM %s", config.dataColumnName(),
                                        config.idColumnName(), getTableName());
      }
      return loadAllRowsSql;
   }

   @Override
   public String getDeleteAllRowsSql() {
      if (deleteAllRows == null) {
         deleteAllRows = "DELETE FROM " + getTableName();
      }
      return deleteAllRows;
   }

   @Override
   public String getSelectExpiredRowsSql() {
      if (selectExpiredRowsSql == null) {
         selectExpiredRowsSql = String.format("%s WHERE %s < ?", getLoadAllRowsSql(), config.timestampColumnName());
      }
      return selectExpiredRowsSql;
   }

   @Override
   public String getDeleteExpiredRowsSql() {
      if (deleteExpiredRowsSql == null) {
         deleteExpiredRowsSql = String.format("DELETE FROM %1$s WHERE %2$s < ? AND %2$s > 0", getTableName(), config.timestampColumnName());
      }
      return deleteExpiredRowsSql;
   }


}