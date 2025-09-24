package org.infinispan.test.hibernate.cache.v62.util;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @since 15.0
 **/
public class ProxyConnection implements Connection {
   private final Connection delegate;
   private final int n;

   ProxyConnection(int n, Connection delegate) {
      this.n = n;
      this.delegate = delegate;
   }

   @Override
   public Statement createStatement() throws SQLException {
      System.out.printf("%d createStatement()%n", n);
      return new ProxyStatement(n, delegate.createStatement());
   }

   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException {
      System.out.printf("%d prepareStatement(%s)%n", n, sql);
      return delegate.prepareStatement(sql);
   }

   @Override
   public CallableStatement prepareCall(String sql) throws SQLException {
      System.out.printf("%d prepareCall(%s)%n", n, sql);
      return delegate.prepareCall(sql);
   }

   @Override
   public String nativeSQL(String sql) throws SQLException {
      System.out.printf("%d nativeSql(%s)%n", n, sql);
      return delegate.nativeSQL(sql);
   }

   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException {
      delegate.setAutoCommit(autoCommit);
   }

   @Override
   public boolean getAutoCommit() throws SQLException {
      return delegate.getAutoCommit();
   }

   @Override
   public void commit() throws SQLException {
      System.out.printf("%d commit()%n", n);
      delegate.commit();
   }

   @Override
   public void rollback() throws SQLException {
      System.out.printf("%d rollback()%n", n);
      delegate.rollback();
   }

   @Override
   public void close() throws SQLException {
      System.out.printf("%d close()%n", n);
      delegate.close();
   }

   @Override
   public boolean isClosed() throws SQLException {
      return delegate.isClosed();
   }

   @Override
   public DatabaseMetaData getMetaData() throws SQLException {
      return delegate.getMetaData();
   }

   @Override
   public void setReadOnly(boolean readOnly) throws SQLException {
      delegate.setReadOnly(readOnly);
   }

   @Override
   public boolean isReadOnly() throws SQLException {
      return delegate.isReadOnly();
   }

   @Override
   public void setCatalog(String catalog) throws SQLException {
      delegate.setCatalog(catalog);
   }

   @Override
   public String getCatalog() throws SQLException {
      return delegate.getCatalog();
   }

   @Override
   public void setTransactionIsolation(int level) throws SQLException {
      delegate.setTransactionIsolation(level);
   }

   @Override
   public int getTransactionIsolation() throws SQLException {
      return delegate.getTransactionIsolation();
   }

   @Override
   public SQLWarning getWarnings() throws SQLException {
      return delegate.getWarnings();
   }

   @Override
   public void clearWarnings() throws SQLException {
      delegate.clearWarnings();
   }

   @Override
   public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      System.out.printf("%d createStatement(%d, %d)%n", n, resultSetType, resultSetConcurrency);
      return delegate.createStatement(resultSetType, resultSetConcurrency);
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      System.out.printf("%d prepareStatement(%s, %d, %d)%n", n, sql, resultSetType, resultSetConcurrency);
      return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
   }

   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      System.out.printf("%d prepareCall(%s, %d, %d)%n", n, sql, resultSetType, resultSetConcurrency);
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
   }

   @Override
   public Map<String, Class<?>> getTypeMap() throws SQLException {
      return delegate.getTypeMap();
   }

   @Override
   public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      delegate.setTypeMap(map);
   }

   @Override
   public void setHoldability(int holdability) throws SQLException {
      delegate.setHoldability(holdability);
   }

   @Override
   public int getHoldability() throws SQLException {
      return delegate.getHoldability();
   }

   @Override
   public Savepoint setSavepoint() throws SQLException {
      return delegate.setSavepoint();
   }

   @Override
   public Savepoint setSavepoint(String name) throws SQLException {
      return delegate.setSavepoint(name);
   }

   @Override
   public void rollback(Savepoint savepoint) throws SQLException {
      delegate.rollback(savepoint);
   }

   @Override
   public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      delegate.releaseSavepoint(savepoint);
   }

   @Override
   public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      System.out.printf("%d createStatement(%d, %d, %d)%n", n, resultSetType, resultSetConcurrency, resultSetHoldability);
      return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      System.out.printf("%d prepareStatement(%s, %d, %d, %d)%n", n, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
      return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      System.out.printf("%d prepareCall(%s, %d, %d, %d)%n", n, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      System.out.printf("%d prepareStatement(%s, %d)%n", n, sql, autoGeneratedKeys);
      return delegate.prepareStatement(sql, autoGeneratedKeys);
   }

   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      System.out.printf("%d prepareStatement(%s, %s)%n", n, sql, Arrays.toString(columnIndexes));
      return delegate.prepareStatement(sql, columnIndexes);
   }

   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      System.out.printf("%d prepareStatement(%s, %s)%n", n, sql, Arrays.toString(columnNames));
      return delegate.prepareStatement(sql, columnNames);
   }

   @Override
   public Clob createClob() throws SQLException {
      return delegate.createClob();
   }

   @Override
   public Blob createBlob() throws SQLException {
      return delegate.createBlob();
   }

   @Override
   public NClob createNClob() throws SQLException {
      return delegate.createNClob();
   }

   @Override
   public SQLXML createSQLXML() throws SQLException {
      return delegate.createSQLXML();
   }

   @Override
   public boolean isValid(int timeout) throws SQLException {
      return delegate.isValid(timeout);
   }

   @Override
   public void setClientInfo(String name, String value) throws SQLClientInfoException {
      delegate.setClientInfo(name, value);
   }

   @Override
   public void setClientInfo(Properties properties) throws SQLClientInfoException {
      delegate.setClientInfo(properties);
   }

   @Override
   public String getClientInfo(String name) throws SQLException {
      return delegate.getClientInfo(name);
   }

   @Override
   public Properties getClientInfo() throws SQLException {
      return delegate.getClientInfo();
   }

   @Override
   public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return delegate.createArrayOf(typeName, elements);
   }

   @Override
   public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return delegate.createStruct(typeName, attributes);
   }

   @Override
   public void setSchema(String schema) throws SQLException {
      delegate.setSchema(schema);
   }

   @Override
   public String getSchema() throws SQLException {
      return delegate.getSchema();
   }

   @Override
   public void abort(Executor executor) throws SQLException {
      delegate.abort(executor);
   }

   @Override
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      delegate.setNetworkTimeout(executor, milliseconds);
   }

   @Override
   public int getNetworkTimeout() throws SQLException {
      return delegate.getNetworkTimeout();
   }

   @Override
   public void beginRequest() throws SQLException {
      delegate.beginRequest();
   }

   @Override
   public void endRequest() throws SQLException {
      delegate.endRequest();
   }

   @Override
   public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
      return delegate.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
   }

   @Override
   public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
      return delegate.setShardingKeyIfValid(shardingKey, timeout);
   }

   @Override
   public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
      delegate.setShardingKey(shardingKey, superShardingKey);
   }

   @Override
   public void setShardingKey(ShardingKey shardingKey) throws SQLException {
      delegate.setShardingKey(shardingKey);
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
   }
}
