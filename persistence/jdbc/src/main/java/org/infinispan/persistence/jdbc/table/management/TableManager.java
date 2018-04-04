package org.infinispan.persistence.jdbc.table.management;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
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

   /**
    * @deprecated Please use {@link AbstractStoreConfiguration#maxBatchSize()} instead.
    */
   @Deprecated
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

   void prepareUpsertStatement(PreparedStatement ps, String key, long timestamp, ByteBuffer byteBuffer) throws SQLException;

   default void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, ByteBuffer byteBuffer) throws SQLException {
      prepareUpsertStatement(ps, key, timestamp, byteBuffer);
   }
}
