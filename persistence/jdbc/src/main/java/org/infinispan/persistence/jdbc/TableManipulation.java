package org.infinispan.persistence.jdbc;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Contains all the logic of manipulating the table, including creating it if needed and access operations like
 * inserting, selecting etc. Used by JDBC based cache persistence.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TableManipulation implements Cloneable {

   private static final Log log = LogFactory.getLog(TableManipulation.class, Log.class);

   public static final int DEFAULT_FETCH_SIZE = 100;

   public static final int DEFAULT_BATCH_SIZE = 128;

   private String identifierQuoteString;
   private String cacheName;
   TableManipulationConfiguration config;

   /*
   * following two params manage creation and destruction during start up/shutdown.
   */
   private ConnectionFactory connectionFactory;

   /* Cache the sql for managing data */
   private String insertRowSql;
   private String updateRowSql;
   private String selectRowSql;
   private String selectIdRowSql;
   private String deleteRowSql;
   private String loadAllRowsSql;
   private String countRowsSql;
   private String loadAllNonExpiredRowsSql;
   private String deleteAllRows;
   private String selectExpiredRowsSql;
   private String deleteExpiredRowsSql;
   private String loadSomeRowsSql;
   private Dialect dialect;
   private String loadAllKeysBinarySql;
   private String loadAllKeysStringSql;

   private TableName tableName;

   public TableManipulation(TableManipulationConfiguration config, Dialect dialect) {
      this.config = config;
      this.dialect = dialect;
   }

   public TableManipulation() {
   }

   public boolean tableExists(Connection connection, TableName tableName) throws PersistenceException {
      if(tableName == null){
         throw new NullPointerException("table name is mandatory");
      }
      ResultSet rs = null;
      try {
         // we need to make sure, that (even if the user has extended permissions) only the tables in current schema are checked
         // explicit set of the schema to the current user one to make sure only tables of the current users are requested
         DatabaseMetaData metaData = connection.getMetaData();
         String schemaPattern = tableName.getSchema();
         if(schemaPattern == null){
            switch (getDialect()) {
               case ORACLE:
                  schemaPattern = metaData.getUserName();
                  break;
               default:
            }
         }
         rs = metaData.getTables(null, schemaPattern, tableName.getName(), new String[] {"TABLE"});
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
      // removed CONSTRAINT clause as this causes problems with some databases, like Informix.
      assertMandatoryElementsPresent();
      String createTableDdl = "CREATE TABLE " + getTableName() + "(" + config.idColumnName() + " " + config.idColumnType()
            + " NOT NULL, " + config.dataColumnName() + " " + config.dataColumnType() + ", "
            + config.timestampColumnName() + " " + config.timestampColumnType() +
            ", PRIMARY KEY (" + config.idColumnName() + "))";
      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", createTableDdl);
      }
      executeUpdateSql(conn, createTableDdl);
   }

   private void assertMandatoryElementsPresent() throws PersistenceException {
      assertNotNull(cacheName, "cacheName needed in order to create table");
   }

   private void assertNotNull(String keyColumnType, String message) throws PersistenceException {
      if (keyColumnType == null || keyColumnType.trim().length() == 0) {
         throw new PersistenceException(message);
      }
   }

   private void executeUpdateSql(Connection conn, String sql) throws PersistenceException {
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

   public void start(ConnectionFactory connectionFactory) throws PersistenceException {
      this.connectionFactory = connectionFactory;
      if (config.createOnStart()) {
         Connection conn = null;
         try {
            conn = this.connectionFactory.getConnection();
            if (!tableExists(conn, getTableName())) {
               createTable(conn);
            }
         } finally {
            this.connectionFactory.releaseConnection(conn);
         }
      }
   }

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

   public String getInsertRowSql() {
      if (insertRowSql == null) {
         insertRowSql = "INSERT INTO " + getTableName() + " (" + config.dataColumnName() + ", " + config.timestampColumnName() + ", " + config.idColumnName() + ") VALUES(?,?,?)";
      }
      return insertRowSql;
   }

   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         switch(getDialect()) {
            case SYBASE:
               updateRowSql = "UPDATE " + getTableName() + " SET " + config.dataColumnName() + " = ? , " + config.timestampColumnName() + "=? WHERE " + config.idColumnName() + " = convert(" + config.idColumnType() + "," + "?)";
               break;
            case POSTGRES:
               updateRowSql = "UPDATE " + getTableName() + " SET " + config.dataColumnName() + " = ? , " + config.timestampColumnName() + "=? WHERE " + config.idColumnName() + " = cast(? as " + config.idColumnType() + ")";
               break;
            default:
               updateRowSql = "UPDATE " + getTableName() + " SET " + config.dataColumnName() + " = ? , " + config.timestampColumnName() + "=? WHERE " + config.idColumnName() + " = ?";
               break;
         }
      }
      return updateRowSql;
   }

   public String getSelectRowSql() {
      if (selectRowSql == null) {
         switch(getDialect()) {
            case SYBASE:
               selectRowSql = "SELECT " + config.idColumnName() + ", " + config.dataColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = convert(" + config.idColumnType() + "," + "?)";
               break;
            case POSTGRES:
               selectRowSql = "SELECT " + config.idColumnName() + ", " + config.dataColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = cast(? as " + config.idColumnType() + ")";
               break;
            default:
               selectRowSql = "SELECT " + config.idColumnName() + ", " + config.dataColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = ?";
               break;
         }
      }
      return selectRowSql;
   }

   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         switch(getDialect()) {
            case SYBASE:
               selectIdRowSql = "SELECT " + config.idColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = convert(" + config.idColumnType() + "," + "?)";
               break;
            case POSTGRES:
               selectIdRowSql = "SELECT " + config.idColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = cast(? as " + config.idColumnType() + ")";
               break;
            default:
               selectIdRowSql = "SELECT " + config.idColumnName() + " FROM " + getTableName() + " WHERE " + config.idColumnName() + " = ?";
               break;
         }
      }
      return selectIdRowSql;
   }

   public String getCountRowsSql() {
      if (countRowsSql == null) {
         countRowsSql = "SELECT COUNT(*) FROM " + getTableName();
      }
      return countRowsSql;
   }

   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         switch(getDialect()) {
            case SYBASE:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + config.idColumnName() + " = convert(" + config.idColumnType() + "," + "?)";
               break;
            case POSTGRES:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + config.idColumnName() + " = cast(? as " + config.idColumnType() + ")";
               break;
            default:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + config.idColumnName() + " = ?";
               break;
         }
      }
      return deleteRowSql;
   }

   public String getLoadNonExpiredAllRowsSql() {
      if (loadAllNonExpiredRowsSql == null) {
         loadAllNonExpiredRowsSql = "SELECT " + config.dataColumnName() + "," + config.idColumnName() + ", " + config.timestampColumnName() + " FROM " + getTableName() + " WHERE " +
               config.timestampColumnName() + " > ? OR " + config.timestampColumnName() + " < 0";
      }
      return loadAllNonExpiredRowsSql;
   }

   public String getLoadAllRowsSql() {
      if (loadAllRowsSql == null) {
         loadAllRowsSql = "SELECT " + config.dataColumnName() + "," + config.idColumnName() + " FROM " + getTableName();
      }
      return loadAllRowsSql;
   }

   public String getDeleteAllRowsSql() {
      if (deleteAllRows == null) {
         deleteAllRows = "DELETE FROM " + getTableName();
      }
      return deleteAllRows;
   }

   public String getSelectExpiredRowsSql() {
      if (selectExpiredRowsSql == null) {
         selectExpiredRowsSql = getLoadAllRowsSql() + " WHERE " + config.timestampColumnName() + "< ?";
      }
      return selectExpiredRowsSql;
   }

   public String getDeleteExpiredRowsSql() {
      if (deleteExpiredRowsSql == null) {
         deleteExpiredRowsSql = "DELETE FROM " + getTableName() + " WHERE " + config.timestampColumnName() + "< ? AND " + config.timestampColumnName() + "> 0";
      }
      return deleteExpiredRowsSql;
   }

   @Override
   public TableManipulation clone() {
      try {
         return (TableManipulation) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException(e);
      }
   }

   public TableName getTableName() {
      if (tableName == null) {
         tableName = new TableName(getIdentifierQuoteString(), config.tableNamePrefix(), cacheName);
      }
      return tableName;
   }

   public boolean tableExists(Connection connection) throws PersistenceException {
      return tableExists(connection, getTableName());
   }

   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
      tableName = null;
   }

   public boolean isVariableLimitSupported() {
      Dialect type = getDialect();
      return !(type == Dialect.DB2 || type == Dialect.DB2_390 || type == Dialect.SYBASE);
   }

   public String getLoadSomeRowsSql() {
      if (loadSomeRowsSql == null) {
         // this stuff is going to be database specific!!
         // see http://stackoverflow.com/questions/595123/is-there-an-ansi-sql-alternative-to-the-mysql-limit-keyword

         switch (getDialect()) {
            case ORACLE:
               loadSomeRowsSql = String.format("SELECT %s, %s FROM (SELECT %s, %s FROM %s) WHERE ROWNUM <= ?", config.dataColumnName(), config.idColumnName(), config.dataColumnName(), config.idColumnName(), getTableName());
               break;
            case DB2:
            case DB2_390:
            case DERBY:
               loadSomeRowsSql = String.format("SELECT %s, %s FROM %s FETCH FIRST ? ROWS ONLY", config.dataColumnName(), config.idColumnName(), getTableName());
               break;
            case INFORMIX:
            case INTERBASE:
            case FIREBIRD:
               loadSomeRowsSql = String.format("SELECT FIRST ? %s, %s FROM %s", config.dataColumnName(), config.idColumnName(), getTableName());
               break;
            case SQL_SERVER:
               loadSomeRowsSql = String.format("SELECT TOP (?) %s, %s FROM %s", config.dataColumnName(), config.idColumnName(), getTableName());
               break;
            case ACCESS:
            case HSQL:
            case SYBASE:
               loadSomeRowsSql = String.format("SELECT TOP ? %s, %s FROM %s", config.dataColumnName(), config.idColumnName(), getTableName());
               break;
            default:
               // the MySQL-style LIMIT clause (works for PostgreSQL too)
               loadSomeRowsSql = String.format("SELECT %s, %s FROM %s LIMIT ?", config.dataColumnName(), config.idColumnName(), getTableName());
               break;
         }

      }
      return loadSomeRowsSql;
   }

   public String getLoadAllKeysBinarySql() {
      if (loadAllKeysBinarySql == null) {
         loadAllKeysBinarySql = String.format("SELECT %s FROM %s", config.dataColumnName(), getTableName());
      }
      return loadAllKeysBinarySql;
   }

   public String getLoadAllKeysStringSql() {
      if (loadAllKeysStringSql == null) {
         loadAllKeysStringSql = String.format("SELECT %s FROM %s", config.idColumnName(), getTableName());
      }
      return loadAllKeysStringSql;
   }

   /**
    * For DB queries the fetch size will be set on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional parameter,
    * if not specified will be defaulted to {@link #DEFAULT_FETCH_SIZE}.
    */
   public int getFetchSize() {
      return config.fetchSize();
   }

   /**
    * When doing repetitive DB inserts  this will be batched according to this parameter. This is an optional parameter,
    * and if it is not specified it will be defaulted to {@link #DEFAULT_BATCH_SIZE}.  Guaranteed to be a power of two.
    */
   public int getBatchSize() {
      return config.batchSize();
   }

   private Dialect getDialect() {
      if (dialect == null) {
         // need to guess from the database type!
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            String dbProduct = connection.getMetaData().getDatabaseProductName();
            dialect = guessDialect(dbProduct);
         } catch (Exception e) {
            log.debug("Unable to guess dialect from JDBC metadata.", e);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
         if (dialect == null) {
            log.debug("Unable to detect database dialect using connection metadata.  Attempting to guess on driver name.");
            try {
               connection = connectionFactory.getConnection();
               String dbProduct = connectionFactory.getConnection().getMetaData().getDriverName();
               dialect = guessDialect(dbProduct);
            } catch (Exception e) {
               log.debug("Unable to guess database dialect from JDBC driver name.", e);
            } finally {
               connectionFactory.releaseConnection(connection);
            }
         }
         if (dialect == null) {
            throw new CacheConfigurationException("Unable to detect database dialect from JDBC driver name or connection metadata.  Please provide this manually using the 'dialect' property in your configuration.  Supported database dialect strings are " + Arrays.toString(Dialect.values()));
         } else {
            log.debugf("Guessing database dialect as '%s'.  If this is incorrect, please specify the correct dialect using the 'dialect' attribute in your configuration.  Supported database dialect strings are %s", dialect, Arrays.toString(Dialect.values()));
         }
      }
      return dialect;
   }

   private Dialect guessDialect(String name) {
      Dialect type = null;
      if (name != null) {
         if (name.toLowerCase().contains("mysql")) {
            type = Dialect.MYSQL;
         } else if (name.toLowerCase().contains("postgres")) {
            type = Dialect.POSTGRES;
         } else if (name.toLowerCase().contains("derby")) {
            type = Dialect.DERBY;
         } else if (name.toLowerCase().contains("hsql") || name.toLowerCase().contains("hypersonic")) {
            type = Dialect.HSQL;
         } else if (name.toLowerCase().contains("h2")) {
            type = Dialect.H2;
         } else if (name.toLowerCase().contains("sqlite")) {
            type = Dialect.SQLITE;
         } else if (name.toLowerCase().contains("db2")) {
            type = Dialect.DB2;
         } else if (name.toLowerCase().contains("informix")) {
            type = Dialect.INFORMIX;
         } else if (name.toLowerCase().contains("interbase")) {
            type = Dialect.INTERBASE;
         } else if (name.toLowerCase().contains("firebird")) {
            type = Dialect.FIREBIRD;
         } else if (name.toLowerCase().contains("sqlserver") || name.toLowerCase().contains("microsoft")) {
            type = Dialect.SQL_SERVER;
         } else if (name.toLowerCase().contains("access")) {
            type = Dialect.ACCESS;
         } else if (name.toLowerCase().contains("oracle")) {
            type = Dialect.ORACLE;
         } else if (name.toLowerCase().contains("adaptive")) {
            type = Dialect.SYBASE;
         }
      }
      return type;
   }

   public String getIdentifierQuoteString() {
      if(identifierQuoteString == null){
         switch (getDialect()) {
            case MYSQL:
               identifierQuoteString = "`";
               break;
            default:
               identifierQuoteString = "\"";
               break;
         }
      }
      return identifierQuoteString;
   }

}

