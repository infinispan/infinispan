package org.infinispan.persistence.jdbc.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * @author William Burns
 */
public interface TableOperations<K, V> {
   MarshallableEntry<K, V> loadEntry(Connection connection, int segment, Object key) throws SQLException;

   default Flowable<K> publishKeys(Supplier<Connection> connectionSupplier, Consumer<Connection> connectionCloser,
         IntSet segments, Predicate<? super K> filter) {
      return publishEntries(connectionSupplier, connectionCloser, segments, filter, false)
            .map(MarshallableEntry::getKey);
   }

   Flowable<MarshallableEntry<K, V>> publishEntries(Supplier<Connection> connectionSupplier,
         Consumer<Connection> connectionCloser, IntSet segments, Predicate<? super K> filter, boolean fetchValue);

   boolean deleteEntry(Connection connection, int segment, Object key) throws SQLException;

   void deleteAllRows(Connection connection) throws SQLException;

   void upsertEntry(Connection connection, int segment, MarshallableEntry<? extends K, ? extends V> entry) throws SQLException;

   long size(Connection connection) throws SQLException;

   void batchUpdates(Connection connection, int writePublisherCount, Publisher<Object> removePublisher,
         Publisher<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) throws SQLException;
}
