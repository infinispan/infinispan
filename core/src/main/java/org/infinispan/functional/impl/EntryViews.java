package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.MetaParamsInternalMetadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

public final class EntryViews {

   private EntryViews() {
      // Cannot be instantiated, it's just a holder class
   }

   public static <K, V> ReadEntryView<K, V> readOnly(CacheEntry<K, V> entry) {
      return new ReadViewImpl<>(entry);
   }

   public static <K, V> WriteEntryView<V> writeOnly(CacheEntry<K, V> entry, ListenerNotifier<K, V> notifier) {
      return new WriteViewImpl<>(entry, notifier);
   }

   public static <K, V> ReadWriteEntryView<K, V> readWrite(CacheEntry<K, V> entry, ListenerNotifier<K, V> notifier) {
      return new ReadWriteViewImpl<>(entry, notifier);
   }

   public static <K, V> ReadEntryView<K, V> noValue(K key) {
      return new NoValueView<>(key);
   }

   private static final class ReadViewImpl<K, V> implements ReadEntryView<K, V> {
      final CacheEntry<K, V> entry;

      private ReadViewImpl(CacheEntry<K, V> entry) {
         this.entry = entry;
      }

      @Override
      public K key() {
         return entry.getKey();
      }

      @Override
      public Optional<V> find() {
         return entry == null ? Optional.empty() : Optional.ofNullable(entry.getValue());
      }

      @Override
      public V get() throws NoSuchElementException {
         if (entry == null || entry.getValue() == null)
            throw new NoSuchElementException("No value present");

         return entry.getValue();
      }

      @Override
      public <T> T getMetaParam(MetaParam.Id<T> id) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(id);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with id=" + id + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(MetaParam.Id<T> id) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(id);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }
   }

   private static final class WriteViewImpl<K, V> implements WriteEntryView<V> {
      final ListenerNotifier<K, V> notifier;
      final CacheEntry<K, V> entry;

      private WriteViewImpl(CacheEntry<K, V> entry, ListenerNotifier<K, V> notifier) {
         this.entry = entry;
         this.notifier = notifier;
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         entry.setValue(value);
         entry.setChanged(true);
         updateEntryMetaParamsIfPresent(entry, metas);

         // Data written, no assumptions about previous value can be made,
         // hence we cannot distinguish between create or update.
         //notifier.notifyOnWrite(EntryViews.readOnly(entry));
         return null;
      }

      @Override
      public Void remove() {
         entry.setRemoved(true);
         entry.setChanged(true);
         // For remove write-only listener events, create a value-less read entry view
         //notifier.notifyOnWrite(EntryViews.noValue(entry.getKey()));
         return null;
      }
   }

   private static final class ReadWriteViewImpl<K, V> implements ReadWriteEntryView<K, V> {
      final ListenerNotifier<K, V> notifier;
      final CacheEntry<K, V> entry;

      private ReadWriteViewImpl(CacheEntry<K, V> entry, ListenerNotifier<K, V> notifier) {
         this.entry = entry;
         this.notifier = notifier;
      }

      @Override
      public K key() {
         return entry.getKey();
      }

      @Override
      public Optional<V> find() {
         return entry == null ? Optional.empty() : Optional.ofNullable(entry.getValue());
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         if (!entry.isCreated()) {
            // TODO: Modify listeners
            //notifier.notifyOnModify(EntryViews.readOnly(key, prev), EntryViews.readOnly(key, iv));
         } else {
            // TODO: Created listeners
            // notifier.notifyOnCreate(EntryViews.readOnly(key, iv));
         }

         entry.setValue(value);
         entry.setChanged(true);
         updateEntryMetaParamsIfPresent(entry, metas);
         return null;
      }

      @Override
      public Void remove() {
         entry.setRemoved(true);
         entry.setChanged(true);
         // For remove write-only listener events, create a value-less read entry view
         //notifier.notifyOnWrite(EntryViews.noValue(entry.getKey()));
         return null;
      }

      @Override
      public <T> T getMetaParam(MetaParam.Id<T> id) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(id);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with id=" + id + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(MetaParam.Id<T> id) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(id);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public V get() throws NoSuchElementException {
         if (entry == null || entry.getValue() == null)
            throw new NoSuchElementException("No value present");

         return entry.getValue();
      }
   }

   public static final class ReadWriteViewImplExternalizer extends AbstractExternalizer<ReadWriteViewImpl> {
      @Override
      public void writeObject(ObjectOutput output, ReadWriteViewImpl object) throws IOException {
         output.writeObject(object.entry);
      }

      @Override
      public ReadWriteViewImpl<?, ?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CacheEntry entry = (CacheEntry) input.readObject();
         return new ReadWriteViewImpl<>(entry, null);
      }

      @Override
      public Set<Class<? extends ReadWriteViewImpl>> getTypeClasses() {
         return Util.<Class<? extends ReadWriteViewImpl>>asSet(ReadWriteViewImpl.class);
      }

      @Override
      public Integer getId() {
         return Ids.READ_WRITE_VIEW_IMPL;
      }
   }

   public static final class NoValueView<K, V> implements ReadEntryView<K, V> {
      final K key;

      public NoValueView(K key) {
         this.key = key;
      }

      @Override
      public K key() {
         return key;
      }

      @Override
      public V get() throws NoSuchElementException {
         throw new NoSuchElementException("No value");
      }

      @Override
      public Optional<V> find() {
         return Optional.empty();
      }

      @Override
      public <T> T getMetaParam(MetaParam.Id<T> id) throws NoSuchElementException {
         throw new NoSuchElementException("No metadata available");
      }

      @Override
      public <T> Optional<T> findMetaParam(MetaParam.Id<T> id) {
         return Optional.empty();
      }
   }

   private static <K, V> void updateEntryMetaParamsIfPresent(CacheEntry<K, V> entry, MetaParam.Writable... metas) {
      // TODO: Deal with entry instances that are MetaParamsCacheEntry and merge meta params
      // e.g. check if meta params exist and if so, merge, but also check for old metadata
      // information and merge it individually

      if (metas.length != 0) {
         MetaParams metaParams = MetaParams.empty();
         metaParams.addMany(metas);
         entry.setMetadata(MetaParamsInternalMetadata.from(metaParams));
      }
   }

}
