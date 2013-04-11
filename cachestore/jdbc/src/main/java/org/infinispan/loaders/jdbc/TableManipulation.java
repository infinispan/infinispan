/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc;

import org.infinispan.config.ConfigurationException;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;

/**
 * Contains all the logic of manipulating the table, including creating it if needed and access operations like
 * inserting, selecting etc. Used by JDBC based cache loaders.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TableManipulation implements Cloneable {

   private static final Log log = LogFactory.getLog(TableManipulation.class, Log.class);

   public static final int DEFAULT_FETCH_SIZE = 100;

   public static final int DEFAULT_BATCH_SIZE = 100;

   private String identifierQuoteString;
   private String idColumnName;
   private String idColumnType;
   private String tableNamePrefix;
   private String cacheName;
   private String dataColumnName;
   private String dataColumnType;
   private String timestampColumnName;
   private String timestampColumnType;
   private int fetchSize = DEFAULT_FETCH_SIZE;
   private int batchSize = DEFAULT_BATCH_SIZE;

   /*
   * following two params manage creation and destruction during start up/shutdown.
   */
   boolean createTableOnStart = true;
   boolean dropTableOnExit = false;
   private ConnectionFactory connectionFactory;

   /* Cache the sql for managing data */
   private String insertRowSql;
   private String updateRowSql;
   private String selectRowSql;
   private String selectIdRowSql;
   private String deleteRowSql;
   private String loadAllRowsSql;
   private String loadAllNonExpiredRowsSql;
   private String deleteAllRows;
   private String selectExpiredRowsSql;
   private String deleteExpiredRowsSql;
   private String loadSomeRowsSql;
   public DatabaseType databaseType;
   private String loadAllKeysBinarySql;
   private String loadAllKeysStringSql;

   private TableName tableName;

   public TableManipulation(String idColumnName, String idColumnType, String tableNamePrefix, String dataColumnName,
                            String dataColumnType, String timestampColumnName, String timestampColumnType) {
      this.idColumnName = idColumnName;
      this.idColumnType = idColumnType;
      this.tableNamePrefix = tableNamePrefix;
      this.dataColumnName = dataColumnName;
      this.dataColumnType = dataColumnType;
      this.timestampColumnName = timestampColumnName;
      this.timestampColumnType = timestampColumnType;
   }

   public TableManipulation() {
   }

   public boolean tableExists(Connection connection, TableName tableName) throws CacheLoaderException {
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
            switch (getDatabaseType()) {
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

   public void createTable(Connection conn) throws CacheLoaderException {
      // removed CONSTRAINT clause as this causes problems with some databases, like Informix.
      assertMandatoryElementsPresent();
      String createTableDdl = "CREATE TABLE " + getTableName() + "(" + idColumnName + " " + idColumnType
            + " NOT NULL, " + dataColumnName + " " + dataColumnType + ", "
            + timestampColumnName + " " + timestampColumnType +
            ", PRIMARY KEY (" + idColumnName + "))";
      if (log.isTraceEnabled()) {
         log.tracef("Creating table with following DDL: '%s'.", createTableDdl);
      }
      executeUpdateSql(conn, createTableDdl);
   }

   private void assertMandatoryElementsPresent() throws CacheLoaderException {
      assertNotNull(idColumnType, "idColumnType needed in order to create table");
      assertNotNull(idColumnName, "idColumnName needed in order to create table");
      assertNotNull(tableNamePrefix, "tableNamePrefix needed in order to create table");
      assertNotNull(cacheName, "cacheName needed in order to create table");
      assertNotNull(dataColumnName, "dataColumnName needed in order to create table");
      assertNotNull(dataColumnType, "dataColumnType needed in order to create table");
      assertNotNull(timestampColumnName, "timestampColumnName needed in order to create table");
      assertNotNull(timestampColumnType, "timestampColumnType needed in order to create table");
   }

   private void assertNotNull(String keyColumnType, String message) throws CacheLoaderException {
      if (keyColumnType == null || keyColumnType.trim().length() == 0) {
         throw new CacheLoaderException(message);
      }
   }

   private void executeUpdateSql(Connection conn, String sql) throws CacheLoaderException {
      Statement statement = null;
      try {
         statement = conn.createStatement();
         statement.executeUpdate(sql);
      } catch (SQLException e) {
         log.errorCreatingTable(sql, e);
         throw new CacheLoaderException(e);
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   public void dropTable(Connection conn) throws CacheLoaderException {
      String dropTableDdl = "DROP TABLE " + getTableName();
      String clearTable = "DELETE FROM " + getTableName();
      executeUpdateSql(conn, clearTable);
      if (log.isTraceEnabled()) {
         log.tracef("Dropping table with following DDL '%s'", dropTableDdl);
      }
      executeUpdateSql(conn, dropTableDdl);
   }

   private static String toLowerCase(String s) {
      return s.toLowerCase(Locale.ENGLISH);
   }

   private static String toUpperCase(String s) {
      return s.toUpperCase(Locale.ENGLISH);
   }

   public void setIdColumnName(String idColumnName) {
      this.idColumnName = idColumnName;
   }

   public void setIdColumnType(String idColumnType) {
      this.idColumnType = idColumnType;
   }

   public void setTableNamePrefix(String tableNamePrefix) {
      this.tableNamePrefix = tableNamePrefix;
   }

   public void setDataColumnName(String dataColumnName) {
      this.dataColumnName = dataColumnName;
   }

   public void setDataColumnType(String dataColumnType) {
      this.dataColumnType = dataColumnType;
   }

   public void setTimestampColumnName(String timestampColumnName) {
      this.timestampColumnName = timestampColumnName;
   }

   public void setTimestampColumnType(String timestampColumnType) {
      this.timestampColumnType = timestampColumnType;
   }

   public boolean isCreateTableOnStart() {
      return createTableOnStart;
   }

   public void setCreateTableOnStart(boolean createTableOnStart) {
      this.createTableOnStart = createTableOnStart;
   }

   public boolean isDropTableOnExit() {
      return dropTableOnExit;
   }

   public void setDropTableOnExit(boolean dropTableOnExit) {
      this.dropTableOnExit = dropTableOnExit;
   }

   public void start(ConnectionFactory connectionFactory) throws CacheLoaderException {
      this.connectionFactory = connectionFactory;
      if (isCreateTableOnStart()) {
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

   public void stop() throws CacheLoaderException {
      if (isDropTableOnExit()) {
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
         insertRowSql = "INSERT INTO " + getTableName() + " (" + dataColumnName + ", " + timestampColumnName + ", " + idColumnName + ") VALUES(?,?,?)";
      }
      return insertRowSql;
   }

   public String getUpdateRowSql() {
      if (updateRowSql == null) {
         switch(getDatabaseType()) {
            case SYBASE:
               updateRowSql = "UPDATE " + getTableName() + " SET " + dataColumnName + " = ? , " + timestampColumnName + "=? WHERE " + idColumnName + " = convert(" + idColumnType + "," + "?)";
               break;
            case POSTGRES:
               updateRowSql = "UPDATE " + getTableName() + " SET " + dataColumnName + " = ? , " + timestampColumnName + "=? WHERE " + idColumnName + " = cast(? as " + idColumnType + ")";
               break;
            default:
               updateRowSql = "UPDATE " + getTableName() + " SET " + dataColumnName + " = ? , " + timestampColumnName + "=? WHERE " + idColumnName + " = ?";
               break;
         }
      }
      return updateRowSql;
   }

   public String getSelectRowSql() {
      if (selectRowSql == null) {
         switch(getDatabaseType()) {
            case SYBASE:
               selectRowSql = "SELECT " + idColumnName + ", " + dataColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = convert(" + idColumnType + "," + "?)";
               break;
            case POSTGRES:
               selectRowSql = "SELECT " + idColumnName + ", " + dataColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = cast(? as " + idColumnType + ")";
               break;
            default:
               selectRowSql = "SELECT " + idColumnName + ", " + dataColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = ?";
               break;
         }
      }
      return selectRowSql;
   }

   public String getSelectIdRowSql() {
      if (selectIdRowSql == null) {
         switch(getDatabaseType()) {
            case SYBASE:
               selectIdRowSql = "SELECT " + idColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = convert(" + idColumnType + "," + "?)";
               break;
            case POSTGRES:
               selectIdRowSql = "SELECT " + idColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = cast(? as " + idColumnType + ")";
               break;
            default:
               selectIdRowSql = "SELECT " + idColumnName + " FROM " + getTableName() + " WHERE " + idColumnName + " = ?";
               break;
         }
      }
      return selectIdRowSql;
   }

   public String getDeleteRowSql() {
      if (deleteRowSql == null) {
         switch(getDatabaseType()) {
            case SYBASE:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + idColumnName + " = convert(" + idColumnType + "," + "?)";
               break;
            case POSTGRES:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + idColumnName + " = cast(? as " + idColumnType + ")";
               break;
            default:
               deleteRowSql = "DELETE FROM " + getTableName() + " WHERE " + idColumnName + " = ?";
               break;
         }
      }
      return deleteRowSql;
   }

   public String getLoadNonExpiredAllRowsSql() {
      if (loadAllNonExpiredRowsSql == null) {
         loadAllNonExpiredRowsSql = "SELECT " + dataColumnName + "," + idColumnName + ", " + timestampColumnName + " FROM " + getTableName() + " WHERE " +
               timestampColumnName + " > ? OR " + timestampColumnName + " < 0";
      }
      return loadAllNonExpiredRowsSql;
   }

   public String getLoadAllRowsSql() {
      if (loadAllRowsSql == null) {
         loadAllRowsSql = "SELECT " + dataColumnName + "," + idColumnName + " FROM " + getTableName();
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
         selectExpiredRowsSql = getLoadAllRowsSql() + " WHERE " + timestampColumnName + "< ?";
      }
      return selectExpiredRowsSql;
   }

   public String getDeleteExpiredRowsSql() {
      if (deleteExpiredRowsSql == null) {
         deleteExpiredRowsSql = "DELETE FROM " + getTableName() + " WHERE " + timestampColumnName + "< ? AND " + timestampColumnName + "> 0";
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
         tableName = new TableName(getIdentifierQuoteString(), tableNamePrefix, cacheName);
      }
      return tableName;
   }

   public String getTableNamePrefix() {
      return tableNamePrefix;
   }

   public boolean tableExists(Connection connection) throws CacheLoaderException {
      return tableExists(connection, getTableName());
   }

   public String getIdColumnName() {
      return idColumnName;
   }

   public String getIdColumnType() {
      return idColumnType;
   }

   public String getDataColumnName() {
      return dataColumnName;
   }

   public String getDataColumnType() {
      return dataColumnType;
   }

   public String getTimestampColumnName() {
      return timestampColumnName;
   }

   public String getTimestampColumnType() {
      return timestampColumnType;
   }

   /**
    * For DB queries (e.g. {@link org.infinispan.loaders.CacheStore#toStream(java.io.ObjectOutput)} ) the fetch size
    * will be set on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional parameter, if not specified will be
    * defaulted to {@link #DEFAULT_FETCH_SIZE}.
    */
   public int getFetchSize() {
      return fetchSize;
   }

   /**
    * @see #getFetchSize()
    */
   public void setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
   }

   /**
    * When doing repetitive DB inserts (e.g. on {@link org.infinispan.loaders.CacheStore#fromStream(java.io.ObjectInput)}
    * this will be batched according to this parameter. This is an optional parameter, and if it is not specified it
    * will be defaulted to {@link #DEFAULT_BATCH_SIZE}.
    */
   public int getBatchSize() {
      return batchSize;
   }

   /**
    * @see #getBatchSize()
    */
   public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
   }

   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
      tableName = null;
   }

   public boolean isVariableLimitSupported() {
      DatabaseType type = getDatabaseType();
      return !(type == DatabaseType.DB2_390 || type == DatabaseType.SYBASE);
   }

   public String getLoadSomeRowsSql() {
      if (loadSomeRowsSql == null) {
         // this stuff is going to be database specific!!
         // see http://stackoverflow.com/questions/595123/is-there-an-ansi-sql-alternative-to-the-mysql-limit-keyword

         switch (getDatabaseType()) {
            case ORACLE:
               loadSomeRowsSql = String.format("SELECT %s, %s FROM (SELECT %s, %s FROM %s) WHERE ROWNUM <= ?", dataColumnName, idColumnName, dataColumnName, idColumnName, getTableName());
               break;
            case DB2:
            case DB2_390:
            case DERBY:
               loadSomeRowsSql = String.format("SELECT %s, %s FROM %s FETCH FIRST ? ROWS ONLY", dataColumnName, idColumnName, getTableName());
               break;
            case INFORMIX:
            case INTERBASE:
            case FIREBIRD:
               loadSomeRowsSql = String.format("SELECT FIRST ? %s, %s FROM %s", dataColumnName, idColumnName, getTableName());
               break;
            case SQL_SERVER:
               loadSomeRowsSql = String.format("SELECT TOP (?) %s, %s FROM %s", dataColumnName, idColumnName, getTableName());
               break;
            case ACCESS:
            case HSQL:
            case SYBASE:
               loadSomeRowsSql = String.format("SELECT TOP ? %s, %s FROM %s", dataColumnName, idColumnName, getTableName());
               break;
            default:
               // the MySQL-style LIMIT clause (works for PostgreSQL too)
               loadSomeRowsSql = String.format("SELECT %s, %s FROM %s LIMIT ?", dataColumnName, idColumnName, getTableName());
               break;
         }

      }
      return loadSomeRowsSql;
   }

   public String getLoadAllKeysBinarySql() {
      if (loadAllKeysBinarySql == null) {
         loadAllKeysBinarySql = String.format("SELECT %s FROM %s", dataColumnName, getTableName());
      }
      return loadAllKeysBinarySql;
   }

   public String getLoadAllKeysStringSql() {
      if (loadAllKeysStringSql == null) {
         loadAllKeysStringSql = String.format("SELECT %s FROM %s", idColumnName, getTableName());
      }
      return loadAllKeysStringSql;
   }

   private DatabaseType getDatabaseType() {
      if (databaseType == null) {
         // need to guess from the database type!
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            String dbProduct = connection.getMetaData().getDatabaseProductName();
            databaseType = guessDatabaseType(dbProduct);
         } catch (Exception e) {
            log.debug("Unable to guess database type from JDBC metadata.", e);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
         if (databaseType == null) {
            log.debug("Unable to detect database type using connection metadata.  Attempting to guess on driver name.");
            try {
               connection = connectionFactory.getConnection();
               String dbProduct = connectionFactory.getConnection().getMetaData().getDriverName();
               databaseType = guessDatabaseType(dbProduct);
            } catch (Exception e) {
               log.debug("Unable to guess database type from JDBC driver name.", e);
            } finally {
               connectionFactory.releaseConnection(connection);
            }
         }
         if (databaseType == null) {
            throw new ConfigurationException("Unable to detect database type from JDBC driver name or connection metadata.  Please provide this manually using the 'databaseType' property in your configuration.  Supported database type strings are " + Arrays.toString(DatabaseType.values()));
         } else {
            log.debugf("Guessing database type as '%s'.  If this is incorrect, please specify the correct type using the 'databaseType' property in your configuration.  Supported database type strings are %s", databaseType, Arrays.toString(DatabaseType.values()));
         }
      }
      return databaseType;
   }

   private DatabaseType guessDatabaseType(String name) {
      DatabaseType type = null;
      if (name != null) {
         if (name.toLowerCase().contains("mysql")) {
            type = DatabaseType.MYSQL;
         } else if (name.toLowerCase().contains("postgres")) {
            type = DatabaseType.POSTGRES;
         } else if (name.toLowerCase().contains("derby")) {
            type = DatabaseType.DERBY;
         } else if (name.toLowerCase().contains("hsql") || name.toLowerCase().contains("hypersonic")) {
            type = DatabaseType.HSQL;
         } else if (name.toLowerCase().contains("h2")) {
            type = DatabaseType.H2;
         } else if (name.toLowerCase().contains("sqlite")) {
            type = DatabaseType.SQLITE;
         } else if (name.toLowerCase().contains("db2")) {
            type = DatabaseType.DB2;
         } else if (name.toLowerCase().contains("informix")) {
            type = DatabaseType.INFORMIX;
         } else if (name.toLowerCase().contains("interbase")) {
            type = DatabaseType.INTERBASE;
         } else if (name.toLowerCase().contains("firebird")) {
            type = DatabaseType.FIREBIRD;
         } else if (name.toLowerCase().contains("sqlserver") || name.toLowerCase().contains("microsoft")) {
            type = DatabaseType.SQL_SERVER;
         } else if (name.toLowerCase().contains("access")) {
            type = DatabaseType.ACCESS;
         } else if (name.toLowerCase().contains("oracle")) {
            type = DatabaseType.ORACLE;
         } else if (name.toLowerCase().contains("adaptive")) {
            type = DatabaseType.SYBASE;
         }
      }
      return type;
   }

   public String getIdentifierQuoteString() {
      if(identifierQuoteString == null){
         switch (getDatabaseType()) {
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

