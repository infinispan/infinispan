package org.infinispan.persistence.jdbc.common.impl;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.internal.functions.Functions;
import jakarta.transaction.Transaction;

public abstract class BaseJdbcStore<K, V, C extends AbstractJdbcStoreConfiguration> implements NonBlockingStore<K, V> {
   protected static final Log log = LogFactory.getLog(BaseJdbcStore.class, Log.class);

   protected ConnectionFactory connectionFactory;
   protected BlockingManager blockingManager;
   protected C config;
   protected TableOperations<K, V> tableOperations;
   protected final Map<Transaction, Connection> transactionConnectionMap = new ConcurrentHashMap<>();

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.TRANSACTIONAL, Characteristic.SHAREABLE);
   }

   Object keyIdentifier(Object key) {
      return key;
   }

   /**
    * Extension point to allow for initializing and creating a table operations object. All variables in the {@link
    * BaseJdbcStore} will be initialized except for {@link #tableOperations} when this is invoked.
    *
    * @param ctx    store context
    * @param config configuration of the store
    * @return the table operations to use for future calls
    * @throws SQLException if any database exception occurs during creation
    */
   protected abstract TableOperations<K, V> createTableOperations(InitializationContext ctx, C config) throws SQLException;

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.config = ctx.getConfiguration();
      blockingManager = ctx.getBlockingManager();

      return blockingManager.runBlocking(() -> {
         try {
            ConnectionFactory factory = ConnectionFactory.getConnectionFactory(config.connectionFactory().connectionFactoryClass());
            factory.start(config.connectionFactory(), factory.getClass().getClassLoader());
            this.connectionFactory = factory;
            tableOperations = createTableOperations(ctx, config);
         } catch (SQLException e) {
            throw new PersistenceException(e);
         }
      }, "jdbcstore-start");
   }

   /**
    * Method to extend to add additional steps when the store is shutting down. This is invoked before the {@link
    * #connectionFactory} is shut down and should not do so.
    */
   protected void extraStopSteps() {

   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> {
         extraStopSteps();
         try {
            log.tracef("Stopping connection factory: %s", connectionFactory);
            if (connectionFactory != null) {
               connectionFactory.stop();
            }
         } catch (Throwable t) {
            log.debug("Exception while stopping", t);
         }
      }, "jdbcstore-stop");
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return blockingManager.supplyBlocking(() -> {
         if (connectionFactory == null)
            return false;

         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            return connection != null && connection.isValid(10);
         } catch (Throwable t) {
            log.debugf(t, "Exception thrown when checking DB availability");
            throw CompletableFutures.asCompletionException(t);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }, "jdbcstore-available");
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return blockingManager.supplyBlocking(() -> {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            return tableOperations.loadEntry(conn, segment, key);
         } catch (SQLException e) {
            Object keyIdentifier = keyIdentifier(key);
            PERSISTENCE.sqlFailureReadingKey(key, keyIdentifier, e);
            throw new PersistenceException(String.format(
                  "SQL error while fetching stored entry with key: %s, lockingKey: %s",
                  key, keyIdentifier), e);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }, "jdbcstore-load");
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return blockingManager.runBlocking(() -> {
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            tableOperations.upsertEntry(connection, segment, entry);
         } catch (SQLException ex) {
            PERSISTENCE.sqlFailureStoringKey(entry.getKey(), ex);
            throw new PersistenceException(String.format("Error while storing string key to database; key: '%s'", entry.getKey()), ex);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }, "jdbcstore-write");
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return blockingManager.supplyBlocking(() -> {
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            return tableOperations.deleteEntry(connection, segment, key);
         } catch (SQLException ex) {
            PERSISTENCE.sqlFailureRemovingKeys(ex);
            throw new PersistenceException(String.format("Error while removing key %s from database", key), ex);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }, "jdbcstore-delete");
   }

   @Override
   public CompletionStage<Void> clear() {
      return blockingManager.runBlocking(() -> {
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            tableOperations.deleteAllRows(connection);
         } catch (SQLException ex) {
            PERSISTENCE.failedClearingJdbcCacheStore(ex);
            throw new PersistenceException("Failed clearing cache store", ex);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }, "jdbcstore-delete");
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount,
         Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      return blockingManager.runBlocking(() -> {
         Connection connection = null;
         try {
            connection = connectionFactory.getConnection();
            tableOperations.batchUpdates(connection, publisherCount, Flowable.fromPublisher(removePublisher)
                  .concatMapEager(Functions.identity(), publisherCount, publisherCount), writePublisher);
         } catch (SQLException e) {
            throw PERSISTENCE.sqlFailureWritingBatch(e);
         } finally {
            connectionFactory.releaseConnection(connection);
         }
      }, "jdbcstore-batch");
   }

   @Override
   public CompletionStage<Void> prepareWithModifications(Transaction tx, int publisherCount,
         Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      return blockingManager.runBlocking(() -> {
         try {
            Connection connection = getTxConnection(tx);
            connection.setAutoCommit(false);
            tableOperations.batchUpdates(connection, publisherCount, Flowable.fromPublisher(removePublisher)
                  .concatMapEager(Functions.identity(), publisherCount, publisherCount), writePublisher);
            // We do not call connection.close() in the event of an exception, as close() on active Tx behaviour is implementation
            // dependent. See https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#close--
         } catch (SQLException e) {
            throw PERSISTENCE.prepareTxFailure(e);
         }
      }, "jdbcstore-prepare");
   }

   @Override
   public CompletionStage<Void> commit(Transaction tx) {
      return blockingManager.runBlocking(() -> {
         Connection connection;
         try {
            connection = getTxConnection(tx);
            connection.commit();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureTxCommit(e);
            throw new PersistenceException(String.format("Error during commit of JDBC transaction (%s)", tx), e);
         } finally {
            destroyTxConnection(tx);
         }
      }, "jdbcstore-commit");
   }

   @Override
   public CompletionStage<Void> rollback(Transaction tx) {
      return blockingManager.runBlocking(() -> {
         Connection connection;
         try {
            connection = getTxConnection(tx);
            connection.rollback();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureTxRollback(e);
            throw new PersistenceException(String.format("Error during rollback of JDBC transaction (%s)", tx), e);
         } finally {
            destroyTxConnection(tx);
         }
      }, "jdbcstore-rollback");
   }

   protected Connection getTxConnection(Transaction tx) {
      Connection connection = transactionConnectionMap.get(tx);
      if (connection == null) {
         connection = connectionFactory.getConnection();
         transactionConnectionMap.put(tx, connection);
      }
      return connection;
   }

   protected void destroyTxConnection(Transaction tx) {
      Connection connection = transactionConnectionMap.remove(tx);
      if (connection != null)
         connectionFactory.releaseConnection(connection);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return blockingManager.blockingPublisher(tableOperations.publishEntries(connectionFactory::getConnection,
            connectionFactory::releaseConnection, segments, filter, includeValues));
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return blockingManager.blockingPublisher(tableOperations.publishKeys(connectionFactory::getConnection,
            connectionFactory::releaseConnection, segments, filter));
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return blockingManager.supplyBlocking(() -> {
         Connection conn = null;
         try {
            conn = connectionFactory.getConnection();
            return tableOperations.size(conn);
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureSize(e);
            throw new PersistenceException("SQL failure while retrieving size", e);
         } finally {
            connectionFactory.releaseConnection(conn);
         }
      }, "jdbcstore-size");
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      return size(segments);
   }
}
