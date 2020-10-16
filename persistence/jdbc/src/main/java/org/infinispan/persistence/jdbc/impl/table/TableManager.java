package org.infinispan.persistence.jdbc.impl.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 */
public interface TableManager {
   int DEFAULT_FETCH_SIZE = 100;

   void start() throws PersistenceException;

   void stop() throws PersistenceException;

   boolean tableExists(Connection connection, TableName tableName);

   default boolean metaTableExists(Connection conn) {
      return tableExists(conn, getMetaTableName());
   }

   void createDataTable(Connection conn) throws PersistenceException;

   void dropDataTable(Connection conn) throws PersistenceException;

   void createMetaTable(Connection conn) throws PersistenceException;

   void dropMetaTable(Connection conn) throws PersistenceException;

   /**
    * Write the latest metadata to the meta table.
    */
   void updateMetaTable(Connection conn) throws PersistenceException;

   Metadata getMetadata(Connection conn) throws PersistenceException;

   default void dropTables(Connection conn) throws PersistenceException {
      dropDataTable(conn);
      dropMetaTable(conn);
   }

   int getFetchSize();

   boolean isUpsertSupported();

   TableName getDataTableName();

   TableName getMetaTableName();

   String getIdentifierQuoteString();

   String getInsertRowSql();

   String getUpdateRowSql();

   String getUpsertRowSql();

   String getSelectRowSql();

   String getSelectIdRowSql();

   String getCountNonExpiredRowsSql();

   String getCountNonExpiredRowsSqlForSegments(int numSegments);

   String getDeleteRowSql();

   String getLoadNonExpiredAllRowsSql();

   String getLoadNonExpiredRowsSqlForSegments(int numSegments);

   String getLoadAllRowsSql();

   String getDeleteAllRowsSql();

   String getDeleteRowsSqlForSegments(int numSegments);

   String getSelectOnlyExpiredRowsSql();

   boolean isStringEncodingRequired();

   String encodeString(String stringToEncode);

   void prepareUpsertStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException;

   void prepareUpdateStatement(PreparedStatement ps, String key, long timestamp, int segment, ByteBuffer byteBuffer) throws SQLException;

   interface Metadata {

      short getVersion();

      int getSegments();
   }
}
