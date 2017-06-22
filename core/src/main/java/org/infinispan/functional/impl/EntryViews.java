package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Entry views implementation class holder.
 *
 * @since 8.0
 */
@Experimental
public final class EntryViews {

   private EntryViews() {
      // Cannot be instantiated, it's just a holder class
   }

   public static <K, V> ReadEntryView<K, V> readOnly(CacheEntry<K, V> entry) {
      return new EntryBackedReadOnlyView<>(entry);
   }

   public static <K, V> ReadEntryView<K, V> readOnly(K key, V value, Metadata metadata) {
      return new ReadOnlySnapshotView<>(key, value, metadata);
   }

   public static <K, V> WriteEntryView<V> writeOnly(CacheEntry<K, V> entry) {
      return new EntryBackedWriteOnlyView<>(entry);
   }

   public static <K, V> ReadWriteEntryView<K, V> readWrite(CacheEntry<K, V> entry) {
      return new EntryBackedReadWriteView<>(entry);
   }

   public static <K, V> ReadWriteEntryView<K, V> readWrite(CacheEntry<K, V> entry, V prevValue, Metadata prevMetadata) {
      return new EntryAndPreviousReadWriteView<>(entry, prevValue, prevMetadata);
   }

   public static <K, V> ReadEntryView<K, V> noValue(K key) {
      return new NoValueReadOnlyView<>(key);
   }

   /**
    * For convenience, a lambda might decide to return the entry view it
    * received as parameter, because that makes easy to return both value and
    * meta parameters back to the client.
    *
    * If the lambda function decides to return an view, launder it into an
    * immutable view to avoid the user trying apply any modifications to the
    * entry view from outside the lambda function.
    *
    * If the view is read-only, capture its data into a snapshot from the
    * cached entry and avoid changing underneath.
    */
   @SuppressWarnings("unchecked")
   public static <R> R snapshot(R ret) {
      if (ret instanceof EntryBackedReadWriteView) {
         EntryBackedReadWriteView view = (EntryBackedReadWriteView) ret;
         return (R) new ReadWriteSnapshotView(view.key(), view.entry.getValue(), view.entry.getMetadata());
      } else if (ret instanceof EntryAndPreviousReadWriteView) {
         EntryAndPreviousReadWriteView view = (EntryAndPreviousReadWriteView) ret;
         return (R) new ReadWriteSnapshotView(view.key(), view.entry.getValue(), view.entry.getMetadata());
      } else if (ret instanceof EntryBackedReadOnlyView) {
         EntryBackedReadOnlyView view = (EntryBackedReadOnlyView) ret;
         return (R) new ReadOnlySnapshotView(view.key(), view.entry.getValue(), view.entry.getMetadata());
      }

      return ret;
   }

   private static final class EntryBackedReadOnlyView<K, V> implements ReadEntryView<K, V> {
      final CacheEntry<K, V> entry;

      private EntryBackedReadOnlyView(CacheEntry<K, V> entry) {
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
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public String toString() {
         return "EntryBackedReadOnlyView{" + "entry=" + entry + '}';
      }
   }

   private static final class ReadOnlySnapshotView<K, V> implements ReadEntryView<K, V> {
      final K key;
      final V value;
      final Metadata metadata;

      private ReadOnlySnapshotView(K key, V value, Metadata metadata) {
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
         return Optional.ofNullable(value);
      }

      // TODO: Duplication
      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public String toString() {
         return "ReadOnlySnapshotView{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            '}';
      }
   }

   private static final class EntryBackedWriteOnlyView<K, V> implements WriteEntryView<V> {
      final CacheEntry<K, V> entry;

      private EntryBackedWriteOnlyView(CacheEntry<K, V> entry) {
         this.entry = entry;
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         entry.setValue(value);
         entry.setChanged(true);
         entry.setRemoved(value == null);
         updateMetaParams(entry, metas);
         return null;
      }

      @Override
      public Void remove() {
         entry.setRemoved(true);
         entry.setChanged(true);
         entry.setValue(null);
         return null;
      }

      @Override
      public String toString() {
         return "EntryBackedWriteOnlyView{" + "entry=" + entry + '}';
      }
   }

   private static final class EntryBackedReadWriteView<K, V> implements ReadWriteEntryView<K, V> {
      final CacheEntry<K, V> entry;

      private EntryBackedReadWriteView(CacheEntry<K, V> entry) {
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
         entry.setCreated(entry.getValue() == null && value != null);
         entry.setValue(value);
         entry.setChanged(true);
         entry.setRemoved(value == null);

         updateMetaParams(entry, metas);
      }

      @Override
      public Void remove() {
         if (!entry.isNull()) {
            entry.setRemoved(true);
            entry.setChanged(true);
            entry.setValue(null);
            entry.setCreated(false);
         }

         return null;
      }

      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
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

      @Override
      public String toString() {
         return "EntryBackedReadWriteView{" + "entry=" + entry + '}';
      }
   }

   private static final class EntryAndPreviousReadWriteView<K, V> implements ReadWriteEntryView<K, V> {
      final CacheEntry<K, V> entry;
      final V prevValue;
      final Metadata prevMetadata;

      private EntryAndPreviousReadWriteView(CacheEntry<K, V> entry, V prevValue, Metadata prevMetadata) {
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
         return Optional.ofNullable(prevValue);
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         setOnly(value, metas);
         return null;
      }

      private void setOnly(V value, MetaParam.Writable[] metas) {
         entry.setValue(value);
         entry.setChanged(true);
         entry.setRemoved(value == null);
         entry.setCreated(prevValue == null && value != null);
         updateMetaParams(entry, metas);
      }

      @Override
      public Void remove() {
         if (!entry.isNull()) {
            entry.setRemoved(true);
            entry.setCreated(false);
            entry.setChanged(true);
            entry.setValue(null);
         }

         return null;
      }

      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
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
         if (prevValue == null) throw new NoSuchElementException();
         return prevValue;
      }

      @Override
      public String toString() {
         return "EntryAndPreviousReadWriteView{" +
            "entry=" + entry +
            ", prevValue=" + prevValue +
            ", prevMetadata=" + prevMetadata +
            '}';
      }
   }

   private static final class NoValueReadOnlyView<K, V> implements ReadEntryView<K, V> {
      final K key;

      public NoValueReadOnlyView(K key) {
         this.key = key;
      }

      @Override
      public K key() {
         return key;
      }

      @Override
      public V get() throws NoSuchElementException {
         throw new NoSuchElementException("No value for key " + key);
      }

      @Override
      public Optional<V> find() {
         return Optional.empty();
      }

      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         return Optional.empty();
      }

      @Override
      public String toString() {
         return "NoValueReadOnlyView{" + "key=" + key + '}';
      }
   }

   private static final class ReadWriteSnapshotView<K, V> implements ReadWriteEntryView<K, V> {
      final K key;
      final V value;
      final Metadata metadata;

      public ReadWriteSnapshotView(K key, V value, Metadata metadata) {
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
         if (value == null)
            throw new NoSuchElementException("No value present");

         return value;
      }

      @Override
      public Optional<V> find() {
         return Optional.ofNullable(value);
      }

      // TODO: Duplication
      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         throw new IllegalStateException(
            "A read-write entry view cannot be modified outside the scope of a lambda");
      }

      @Override
      public Void remove() {
         throw new IllegalStateException(
            "A read-write entry view cannot be modified outside the scope of a lambda");
      }

      @Override
      public String toString() {
         return "ReadWriteSnapshotView{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            '}';
      }
   }

   private static <K, V> void updateMetaParams(CacheEntry<K, V> entry, MetaParam.Writable[] metas) {
      // TODO: Deal with entry instances that are MetaParamsCacheEntry and merge meta params
      // e.g. check if meta params exist and if so, merge, but also check for old metadata
      // information and merge it individually

      Optional<EntryVersion> version = Optional.ofNullable(entry.getMetadata()).map(m -> m.version());
      MetaParams metaParams = MetaParams.empty();
      if (version.isPresent()) {
         metaParams.add(new MetaParam.MetaEntryVersion(version.get()));
      }
      if (metas.length != 0) {
         metaParams.addMany(metas);
      }

      entry.setMetadata(MetaParamsInternalMetadata.from(metaParams));
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

   public static final class ReadOnlySnapshotViewExternalizer implements AdvancedExternalizer<ReadOnlySnapshotView> {
      @Override
      public Set<Class<? extends ReadOnlySnapshotView>> getTypeClasses() {
         return Util.asSet(ReadOnlySnapshotView.class);
      }

      @Override
      public Integer getId() {
         return Ids.READ_ONLY_SNAPSHOT_VIEW;
      }

      @Override
      public void writeObject(ObjectOutput output, ReadOnlySnapshotView object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
         output.writeObject(object.metadata);
      }

      @Override
      public ReadOnlySnapshotView readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new ReadOnlySnapshotView<>(key, value, metadata);
      }
   }

   public static final class NoValueReadOnlyViewExternalizer implements AdvancedExternalizer<NoValueReadOnlyView> {

      @Override
      public Set<Class<? extends NoValueReadOnlyView>> getTypeClasses() {
         return Util.asSet(NoValueReadOnlyView.class);
      }

      @Override
      public Integer getId() {
         return Ids.NO_VALUE_READ_ONLY_VIEW;
      }

      @Override
      public void writeObject(ObjectOutput output, NoValueReadOnlyView object) throws IOException {
         output.writeObject(object.key);
      }

      @Override
      public NoValueReadOnlyView readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new NoValueReadOnlyView(input.readObject());
      }
   }

   // Externalizer class defined outside of externalized class to avoid having
   // to making externalized class public, since that would leak internal impl.
   public static final class ReadWriteSnapshotViewExternalizer
            extends AbstractExternalizer<ReadWriteSnapshotView> {
      @Override
      public Integer getId() {
         return Ids.READ_WRITE_SNAPSHOT_VIEW;
      }

      @Override @SuppressWarnings("unchecked")
      public Set<Class<? extends ReadWriteSnapshotView>> getTypeClasses() {
         return Util.asSet(ReadWriteSnapshotView.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ReadWriteSnapshotView obj) throws IOException {
         output.writeObject(obj.key);
         output.writeObject(obj.value);
         output.writeObject(obj.metadata);
      }

      @Override @SuppressWarnings("unchecked")
      public ReadWriteSnapshotView readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new ReadWriteSnapshotView(key, value, metadata);
      }
   }

}
