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

   boolean tableExists(Connection connection) throws PersistenceException;

   boolean tableExists(Connection connection, TableName tableName) throws PersistenceException;

   void createTable(Connection conn) throws PersistenceException;

   void dropTable(Connection conn) throws PersistenceException;

   int getFetchSize();

   boolean isUpsertSupported();

   TableName getTableName();

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
}
