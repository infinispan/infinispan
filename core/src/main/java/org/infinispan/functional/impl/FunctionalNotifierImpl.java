package org.infinispan.functional.impl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.commons.util.Experimental;

/**
 * @since 8.0
 */
@Experimental
public final class FunctionalNotifierImpl<K, V> implements FunctionalNotifier<K, V> {

   final List<Consumer<ReadEntryView<K, V>>> onCreates = new CopyOnWriteArrayList<>();
   final List<BiConsumer<ReadEntryView<K, V>, ReadEntryView<K, V>>> onModifies = new CopyOnWriteArrayList<>();
   final List<Consumer<ReadEntryView<K, V>>> onRemoves = new CopyOnWriteArrayList<>();
   final List<Consumer<ReadEntryView<K, V>>> onWrites = new CopyOnWriteArrayList<>();

   final List<ReadWriteListener<K, V>> rwListeners = new CopyOnWriteArrayList<>();
   final List<WriteListener<K, V>> writeListeners = new CopyOnWriteArrayList<>();

   @Override
   public AutoCloseable add(WriteListener<K, V> l) {
      writeListeners.add(l);
      return new ListenerCloseable<>(l, writeListeners);
   }

   @Override
   public AutoCloseable add(ReadWriteListener<K, V> l) {
      rwListeners.add(l);
      return new ListenerCloseable<>(l, rwListeners);
   }

   @Override
   public AutoCloseable onCreate(Consumer<ReadEntryView<K, V>> f) {
      onCreates.add(f);
      return new ListenerCloseable<>(f, onCreates);
   }

   @Override
   public AutoCloseable onModify(BiConsumer<ReadEntryView<K, V>, ReadEntryView<K, V>> f) {
      onModifies.add(f);
      return new ListenerCloseable<>(f, onModifies);
   }

   @Override
   public AutoCloseable onRemove(Consumer<ReadEntryView<K, V>> f) {
      onRemoves.add(f);
      return new ListenerCloseable<>(f, onRemoves);
   }

   @Override
   public AutoCloseable onWrite(Consumer<ReadEntryView<K, V>> f) {
      onWrites.add(f);
      return new ListenerCloseable<>(f, onWrites);
   }

   @Override
   public void notifyOnCreate(ReadEntryView<K, V> created) {
      onCreates.forEach(c -> c.accept(created));
      rwListeners.forEach(rwl -> rwl.onCreate(created));
   }

   @Override
   public void notifyOnModify(ReadEntryView<K, V> before, ReadEntryView<K, V> after) {
      onModifies.forEach(c -> c.accept(before, after));
      rwListeners.forEach(rwl -> rwl.onModify(before, after));
   }

   @Override
   public void notifyOnRemove(ReadEntryView<K, V> removed) {
      onRemoves.forEach(c -> c.accept(removed));
      rwListeners.forEach(rwl -> rwl.onRemove(removed));
   }

   @Override
   public void notifyOnWrite(Supplier<ReadEntryView<K, V>> write) {
      if (!onWrites.isEmpty()) onWrites.forEach(c -> c.accept(write.get()));
      if (!writeListeners.isEmpty()) writeListeners.forEach(wl -> wl.onWrite(write.get()));
   }

   private static final class ListenerCloseable<T> implements AutoCloseable {
      final T f;
      final List<T> list;

      private ListenerCloseable(T f, List<T> list) {
         this.f = f;
         this.list = list;
      }

      @Override
      public void close() throws Exception {
         list.remove(f);
      }
   }

}
