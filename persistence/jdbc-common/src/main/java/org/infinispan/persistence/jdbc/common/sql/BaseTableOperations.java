package org.infinispan.persistence.jdbc.common.sql;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

public abstract class BaseTableOperations<K, V> implements TableOperations<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   protected final AbstractJdbcStoreConfiguration<?> configuration;

   public BaseTableOperations(AbstractJdbcStoreConfiguration<?> configuration) {
      this.configuration = configuration;
   }

   public abstract String getSelectRowSql();

   public abstract String getSelectAllSql(IntSet segments);

   public abstract String getDeleteRowSql();

   public abstract String getDeleteAllSql();

   public abstract String getUpsertRowSql();

   public abstract String getSizeSql();

   protected abstract MarshallableEntry<K, V> entryFromResultSet(ResultSet rs, Object keyIfProvided, boolean fetchValue,
         Predicate<? super K> keyPredicate) throws SQLException;

   protected abstract void prepareKeyStatement(PreparedStatement ps, Object key) throws SQLException;

   protected abstract void prepareValueStatement(PreparedStatement ps, int segment, MarshallableEntry<? extends K, ? extends V> entry) throws SQLException;

   protected void prepareSizeStatement(PreparedStatement ps) throws SQLException {
      // Do nothing by default
   }

   protected void preparePublishStatement(PreparedStatement ps, IntSet segments) throws SQLException {
      // Do nothing by default
   }

   @Override
   public MarshallableEntry<K, V> loadEntry(Connection connection, int segment, Object key) throws SQLException {
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String selectSql = getSelectRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running select row sql '%s'", selectSql);
         }
         ps = connection.prepareStatement(selectSql);
         ps.setQueryTimeout(configuration.readQueryTimeout());
         prepareKeyStatement(ps, key);
         rs = ps.executeQuery();
         if (rs.next()) {
            return entryFromResultSet(rs, key, true, null);
         }
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
      }
      return null;
   }

   @Override
   public boolean deleteEntry(Connection connection, int segment, Object key) throws SQLException {
      PreparedStatement ps = null;
      try {
         String deleteSql = getDeleteRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running delete row sql '%s'", deleteSql);
         }
         ps = connection.prepareStatement(deleteSql);
         ps.setQueryTimeout(configuration.writeQueryTimeout());
         prepareKeyStatement(ps, key);
         return ps.executeUpdate() == 1;
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @Override
   public void deleteAllRows(Connection connection) throws SQLException {
      Statement statement = null;
      try {
         String deleteAllSql = getDeleteAllSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running delete all sql '%s'", deleteAllSql);
         }
         statement = connection.createStatement();
         int result = statement.executeUpdate(deleteAllSql);
         if (log.isTraceEnabled()) {
            log.tracef("Successfully removed %d rows.", result);
         }
      } finally {
         JdbcUtil.safeClose(statement);
      }
   }

   @Override
   public void upsertEntry(Connection connection, int segment, MarshallableEntry<? extends K, ? extends V> entry) throws SQLException {
      PreparedStatement ps = null;
      try {
         String upsertSql = getUpsertRowSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running upsert row sql '%s'", upsertSql);
         }
         ps = connection.prepareStatement(upsertSql);
         ps.setQueryTimeout(configuration.writeQueryTimeout());
         prepareValueStatement(ps, segment, entry);
         ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
   }

   @Override
   public long size(Connection connection) throws SQLException {
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sizeSql = getSizeSql();
         if (log.isTraceEnabled()) {
            log.tracef("Running count sql '%s'", sizeSql);
         }
         ps = connection.prepareStatement(sizeSql);
         prepareSizeStatement(ps);
         rs = ps.executeQuery();
         rs.next();
         return rs.getInt(1);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
      }
   }

   // This method invokes some blocking methods, but this method is itself only blocking
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   @Override
   public void batchUpdates(Connection connection, int writePublisherCount, Publisher<Object> removePublisher,
         Publisher<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) throws SQLException {
      String upsertSql = getUpsertRowSql();
      String deleteSql = getDeleteRowSql();
      if (log.isTraceEnabled()) {
         log.tracef("Running batch upsert sql '%s'", upsertSql);
         log.tracef("Running batch delete sql '%s'", deleteSql);
      }
      try (PreparedStatement upsertBatch = connection.prepareStatement(upsertSql);
           PreparedStatement deleteBatch = connection.prepareStatement(deleteSql)) {

         // Note this one is non blocking as we need to subscribe to both Publishers before anything is processed.
         CompletionStage<Void> removeStage = Flowable.fromPublisher(removePublisher)
               .doOnNext(key -> {
                  prepareKeyStatement(deleteBatch, key);
                  deleteBatch.addBatch();
               }).lastElement()
               .doAfterSuccess(ignore -> deleteBatch.executeBatch())
               .flatMapCompletable(ignore -> Completable.complete())
               .toCompletionStage(null);

         ByRef<Throwable> throwableRef = new ByRef<>(null);
         ByRef<Object> hadValue = new ByRef<>(null);
         Flowable.fromPublisher(writePublisher)
               .concatMapEager(sp ->
                     Flowable.fromPublisher(sp)
                           .doOnNext(me -> {
                              prepareValueStatement(upsertBatch, sp.getSegment(), me);
                              upsertBatch.addBatch();
                           }), writePublisherCount, writePublisherCount
               ).lastElement()
               .blockingSubscribe(hadValue::set, throwableRef::set);
         if (hadValue.get() != null) {
            upsertBatch.executeBatch();
         }

         Throwable t = throwableRef.get();
         if (t != null) {
            if (t instanceof SQLException) {
               throw (SQLException) t;
            }
            throw Util.rewrapAsCacheException(t);
         }

         CompletionStages.join(removeStage);
      }
   }

   @Override
   public Flowable<MarshallableEntry<K, V>> publishEntries(Supplier<Connection> connectionSupplier,
         Consumer<Connection> connectionCloser, IntSet segments, Predicate<? super K> filter, boolean fetchValue) {
      return Flowable.using(() -> {
         String selectSql = getSelectAllSql(segments);
         if (log.isTraceEnabled()) {
            log.tracef("Running select all sql '%s'", selectSql);
         }
         return new FlowableConnection(connectionSupplier.get(), connectionCloser, selectSql);
      }, fc -> {
         PreparedStatement ps = fc.statement;
         preparePublishStatement(ps, segments);
         ps.setFetchSize(configuration.maxBatchSize());
         ResultSet rs = ps.executeQuery();
         return Flowable.fromIterable(() -> new ResultSetEntryIterator(rs, filter, fetchValue))
               .doFinally(() -> JdbcUtil.safeClose(rs));
      }, FlowableConnection::close, /* Not eager so result set is closed first */false);
   }

   protected static class FlowableConnection {
      protected final boolean autoCommit;
      protected final Connection connection;
      protected final Consumer<Connection> connectionCloser;
      protected final PreparedStatement statement;

      public FlowableConnection(Connection connection, Consumer<Connection> connectionCloser, String sql) throws SQLException {
         this.connection = connection;
         this.connectionCloser = connectionCloser;
         this.autoCommit = connection.getAutoCommit();
         this.statement = connection.prepareStatement(sql);

         // Some JDBC drivers require auto commit disabled to do paging, however before calling setAutoCommit(false)
         // we must ensure that we're not running in a managed transaction by ensuring that getAutoCommit is true.
         // Without this check an exception would be thrown when calling setAutoCommit(false) during a managed transaction.
         if (autoCommit)
            connection.setAutoCommit(false);
      }

      public boolean isAutoCommit() {
         return autoCommit;
      }

      public Connection getConnection() {
         return connection;
      }

      public Consumer<Connection> getConnectionCloser() {
         return connectionCloser;
      }

      public PreparedStatement getStatement() {
         return statement;
      }

      public void close() {
         JdbcUtil.safeClose(statement);
         if (autoCommit) {
            try {
               connection.rollback();
            } catch (SQLException e) {
               PERSISTENCE.sqlFailureTxRollback(e);
            }
         }
         connectionCloser.accept(connection);
      }
   }

   protected class ResultSetEntryIterator extends AbstractIterator<MarshallableEntry<K, V>> {
      private final ResultSet rs;
      private final Predicate<? super K> filter;
      private final boolean fetchValue;

      public ResultSetEntryIterator(ResultSet rs, Predicate<? super K> filter, boolean fetchValue) {
         this.rs = rs;
         this.filter = filter;
         this.fetchValue = fetchValue;
      }

      @Override
      protected MarshallableEntry<K, V> getNext() {
         try {
            while (rs.next()) {
               MarshallableEntry<K, V> entry = entryFromResultSet(rs, null, fetchValue, filter);
               if (entry != null) {
                  return entry;
               }
            }
         } catch (SQLException e) {
            throw new CacheException(e);
         }
         return null;
      }
   }
}
