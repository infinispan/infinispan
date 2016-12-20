package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.spi.PersistenceException;

import java.sql.Connection;

/**
 * @author Ryan Emerson
 */
public interface TableManager {
   int DEFAULT_FETCH_SIZE = 100;
   int DEFAULT_BATCH_SIZE = 128;

   void start() throws PersistenceException;

   void stop() throws PersistenceException;

   boolean tableExists(Connection connection) throws PersistenceException;

   boolean tableExists(Connection connection, TableName tableName) throws PersistenceException;

   void createTable(Connection conn) throws PersistenceException;

   void dropTable(Connection conn) throws PersistenceException;

   void setCacheName(String cacheName);

   int getFetchSize();

   int getBatchSize();

   TableName getTableName();

   String getIdentifierQuoteString();

   String getInsertRowSql();

   String getUpdateRowSql();

   String getSelectRowSql();

   String getSelectIdRowSql();

   String getCountRowsSql();

   String getDeleteRowSql();

   String getLoadNonExpiredAllRowsSql();

   String getLoadAllRowsSql();

   String getDeleteAllRowsSql();

   String getSelectExpiredRowsSql();

   String getDeleteExpiredRowsSql();

   boolean isStringEncodingRequired();

   String encodeString(String stringToEncode);
}