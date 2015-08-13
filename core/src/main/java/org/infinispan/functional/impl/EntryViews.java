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
      return new CacheEntryReadEntryView<>(entry);
   }

   public static <K, V> WriteEntryView<V> writeOnly(CacheEntry<K, V> entry) {
      return new CacheEntryWriteEntryView<>(entry);
   }

   public static <K, V> ReadWriteEntryView<K, V> readWrite(CacheEntry<K, V> entry) {
      return new CacheEntryReadWriteEntryView<>(entry);
   }

   public static <K, V> ReadWriteEntryView<K, V> readWrite(CacheEntry<K, V> entry, V prevValue, Metadata prevMetadata) {
      return new PreviousReadWriteEntryView<>(entry, prevValue, prevMetadata);
   }

   public static <K, V> ReadEntryView<K, V> noValue(K key) {
      return new NoValueView<>(key);
   }

   public static <K, V> ReadEntryView<K, V> readOnly(K key, V value, Metadata metadata) {
      return new ReadViewMetadata<>(key, value, metadata);
   }

   private static final class CacheEntryReadEntryView<K, V> implements ReadEntryView<K, V> {
      final CacheEntry<K, V> entry;

      private CacheEntryReadEntryView(CacheEntry<K, V> entry) {
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
      public <T> T getMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with type=" + type + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }
   }

   private static final class ReadViewMetadata<K, V> implements ReadEntryView<K, V> {
      final K key;
      final V value;
      final Metadata metadata;

      private ReadViewMetadata(K key, V value, Metadata metadata) {
         this.key = key;
         this.value = value;
         this.metadata = metadata;
      }

      @Override
      public K key() {
         return key;
      }

      @Override
      public V get() throws NoSuchElementException {
         if (value == null) throw new NoSuchElementException("No value");
         return value;
      }

      @Override
      public Optional<V> find() {
         return value == null ? Optional.empty() : Optional.ofNullable(value);
      }

      @Override
      public <T> T getMetaParam(Class<T> type) throws NoSuchElementException {
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with type=" + type + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(Class<T> type) {
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }
   }

   private static final class CacheEntryWriteEntryView<K, V> implements WriteEntryView<V> {
      final CacheEntry<K, V> entry;

      private CacheEntryWriteEntryView(CacheEntry<K, V> entry) {
         this.entry = entry;
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         entry.setValue(value);
         entry.setChanged(true);
         updateMetaParams(entry, metas);
         return null;
      }

      @Override
      public Void remove() {
         entry.setRemoved(true);
         entry.setChanged(true);
         return null;
      }
   }

   private static final class CacheEntryReadWriteEntryView<K, V> implements ReadWriteEntryView<K, V> {
      final CacheEntry<K, V> entry;

      private CacheEntryReadWriteEntryView(CacheEntry<K, V> entry) {
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
      public Void set(V value, MetaParam.Writable... metas) {
         setOnly(value, metas);
         return null;
      }

      private void setOnly(V value, MetaParam.Writable[] metas) {
         entry.setValue(value);
         entry.setChanged(true);
         updateMetaParams(entry, metas);
      }

      @Override
      public Void remove() {
         if (!entry.isNull()) {
            entry.setRemoved(true);
            entry.setChanged(true);
         }

         return null;
      }

      @Override
      public <T> T getMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with type=" + type + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
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

   private static final class PreviousReadWriteEntryView<K, V> implements ReadWriteEntryView<K, V> {
      final CacheEntry<K, V> entry;
      final V prevValue;
      final Metadata prevMetadata;

      private PreviousReadWriteEntryView(CacheEntry<K, V> entry,
            V prevValue, Metadata prevMetadata) {
         this.entry = entry;
         this.prevValue = prevValue;
         this.prevMetadata = prevMetadata;
      }

      @Override
      public K key() {
         return entry.getKey();
      }

      @Override
      public Optional<V> find() {
         return prevValue == null ? Optional.empty() : Optional.ofNullable(prevValue);
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         setOnly(value, metas);
         return null;
      }

      private void setOnly(V value, MetaParam.Writable[] metas) {
         entry.setValue(value);
         entry.setChanged(true);
         updateMetaParams(entry, metas);
      }

      @Override
      public Void remove() {
         if (!entry.isNull()) {
            entry.setRemoved(true);
            entry.setChanged(true);
         }

         return null;
      }

      @Override
      public <T> T getMetaParam(Class<T> type) {
         Metadata metadata = prevMetadata; // Use previous metadata
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.getMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         throw new NoSuchElementException("Metadata with type=" + type + " not found");
      }

      @Override
      public <T> Optional<T> findMetaParam(Class<T> type) {
         Metadata metadata = prevMetadata; // Use previous metadata
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public V get() throws NoSuchElementException {
         return prevValue;
      }
   }

   public static final class ReadWriteViewImplExternalizer extends AbstractExternalizer<CacheEntryReadWriteEntryView> {
      @Override
      public void writeObject(ObjectOutput output, CacheEntryReadWriteEntryView object) throws IOException {
         output.writeObject(object.entry);
      }

      @Override
      public CacheEntryReadWriteEntryView readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CacheEntry entry = (CacheEntry) input.readObject();
         return new CacheEntryReadWriteEntryView(entry);
      }

      @Override
      public Set<Class<? extends CacheEntryReadWriteEntryView>> getTypeClasses() {
         return Util.<Class<? extends CacheEntryReadWriteEntryView>>asSet(CacheEntryReadWriteEntryView.class);
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
      public <T> T getMetaParam(Class<T> type) throws NoSuchElementException {
         throw new NoSuchElementException("No metadata available for type=" + type);
      }

      @Override
      public <T> Optional<T> findMetaParam(Class<T> type) {
         return Optional.empty();
      }
   }

   private static <K, V> MetaParams updateMetaParams(CacheEntry<K, V> entry, MetaParam.Writable[] metas) {
      // TODO: Deal with entry instances that are MetaParamsCacheEntry and merge meta params
      // e.g. check if meta params exist and if so, merge, but also check for old metadata
      // information and merge it individually

      if (metas.length != 0) {
         MetaParams metaParams = MetaParams.empty();
         metaParams.addMany(metas);
         entry.setMetadata(MetaParamsInternalMetadata.from(metaParams));
         return metaParams;
      }

      MetaParams empty = MetaParams.empty();
      entry.setMetadata(MetaParamsInternalMetadata.from(empty));
      return empty;
   }

   private static <K, V> MetaParams extractMetaParams(CacheEntry<K, V> entry) {
      // TODO: Deal with entry instances that are MetaParamsCacheEntry and merge meta params
      // e.g. check if meta params exist and if so, merge, but also check for old metadata
      // information and merge it individually

      Metadata metadata = entry.getMetadata();
      if (metadata instanceof MetaParamsInternalMetadata) {
         MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
         return metaParamsMetadata.params;
      }

      return MetaParams.empty();
   }


}
