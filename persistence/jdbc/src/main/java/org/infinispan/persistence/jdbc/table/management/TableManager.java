package org.infinispan.persistence.jdbc.table.management;

import java.sql.Connection;

import org.infinispan.persistence.spi.PersistenceException;

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

   boolean isUpsertSupported();

   TableName getTableName();

   String getIdentifierQuoteString();

   String getInsertRowSql();

   String getUpdateRowSql();

   String getUpsertRowSql();

   String getSelectRowSql();

   String getSelectMultipleRowSql(int numberOfParams);

   String getSelectIdRowSql();

   String getCountRowsSql();

   String getDeleteRowSql();

   String getLoadNonExpiredAllRowsSql();

   String getLoadAllRowsSql();

   String getDeleteAllRowsSql();

   String getSelectExpiredBucketsSql();

   String getSelectOnlyExpiredRowsSql();

   boolean isStringEncodingRequired();

   String encodeString(String stringToEncode);
}
