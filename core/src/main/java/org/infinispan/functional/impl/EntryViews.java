package org.infinispan.functional.impl;

import static org.infinispan.metadata.Metadatas.updateMetadata;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Entry views implementation class holder.
 *
 * @since 8.0
 */
// TODO adapt for user marshaller
@Experimental
public final class EntryViews {

   private EntryViews() {
      // Cannot be instantiated, it's just a holder class
   }

   public static <K, V> ReadEntryView<K, V> readOnly(CacheEntry<K, V> entry, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return new EntryBackedReadOnlyView<>(entry, keyDataConversion, valueDataConversion);
   }

   public static <K, V> ReadEntryView<K, V> readOnly(CacheEntry entry) {
      return new EntryBackedReadOnlyView<>(entry, DataConversion.DEFAULT_KEY, DataConversion.DEFAULT_VALUE);
   }

   public static <K, V> ReadEntryView<K, V> readOnly(K key, V value, Metadata metadata) {
      return new ReadOnlySnapshotView<>(key, value, metadata);
   }

   public static <K, V> WriteEntryView<K, V> writeOnly(CacheEntry entry, DataConversion valueDataConversion) {
      return new EntryBackedWriteOnlyView<>(entry, valueDataConversion);
   }

   public static <K, V> AccessLoggingReadWriteView<K, V> readWrite(MVCCEntry entry, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return new EntryBackedReadWriteView<>(entry, keyDataConversion, valueDataConversion);
   }

   public static <K, V> AccessLoggingReadWriteView<K, V> readWrite(MVCCEntry entry, Object prevValue, Metadata prevMetadata, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return new EntryAndPreviousReadWriteView<>(entry, prevValue, prevMetadata, keyDataConversion, valueDataConversion);
   }

   public static <K, V> ReadEntryView<K, V> noValue(Object key) {
      return new NoValueReadOnlyView<>(key, null);
   }

   public static <K, V> ReadEntryView<K, V> noValue(Object key, DataConversion keyDataConversion) {
      return new NoValueReadOnlyView<>(key, keyDataConversion);
   }

   /**
    * For convenience, a lambda might decide to return the entry view it received as parameter, because that makes easy
    * to return both value and meta parameters back to the client.
    * <p>
    * If the lambda function decides to return an view, launder it into an immutable view to avoid the user trying apply
    * any modifications to the entry view from outside the lambda function.
    * <p>
    * If the view is read-only, capture its data into a snapshot from the cached entry and avoid changing underneath.
    */
   @SuppressWarnings("unchecked")
   public static <R> R snapshot(R ret) {
      if (ret instanceof EntryBackedReadWriteView) {
         EntryBackedReadWriteView view = (EntryBackedReadWriteView) ret;
         return (R) new ReadWriteSnapshotView(view.key(), view.find().orElse(null), view.entry.getMetadata());
      } else if (ret instanceof EntryAndPreviousReadWriteView) {
         EntryAndPreviousReadWriteView view = (EntryAndPreviousReadWriteView) ret;
         return (R) new ReadWriteSnapshotView(view.key(), view.getCurrentValue(), view.entry.getMetadata());
      } else if (ret instanceof EntryBackedReadOnlyView) {
         EntryBackedReadOnlyView view = (EntryBackedReadOnlyView) ret;
         return (R) new ReadOnlySnapshotView(view.key(), view.find().orElse(null), view.entry.getMetadata());
      } else if (ret instanceof NoValueReadOnlyView) {
         NoValueReadOnlyView view = (NoValueReadOnlyView) ret;
         return (R) new ReadOnlySnapshotView(view.key(), null, null);
      }

      return ret;
   }

   public interface AccessLoggingReadWriteView<K, V> extends ReadWriteEntryView<K, V> {
      boolean isRead();
   }

   private static final class EntryBackedReadOnlyView<K, V> implements ReadEntryView<K, V> {
      final CacheEntry<K, V> entry;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;

      private EntryBackedReadOnlyView(CacheEntry<K, V> entry, DataConversion keyDataConversion, DataConversion valueDataConversion) {
         this.entry = entry;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
      }

      @Override
      public K key() {
         return (K) keyDataConversion.fromStorage(entry.getKey());
      }

      @Override
      public Optional<V> find() {
         return entry == null ? Optional.empty() : Optional.ofNullable((V) valueDataConversion.fromStorage(entry.getValue()));
      }

      @Override
      public V get() throws NoSuchElementException {
         if (entry == null || entry.getValue() == null)
            throw new NoSuchElementException("No value present");

         return (V) valueDataConversion.fromStorage(entry.getValue());
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

   private static final class EntryBackedWriteOnlyView<K, V> implements WriteEntryView<K, V> {
      final CacheEntry entry;
      private final DataConversion valueDataConversion;

      private EntryBackedWriteOnlyView(CacheEntry entry, DataConversion valueDataConversion) {
         this.entry = entry;
         this.valueDataConversion = valueDataConversion;
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         setValue(value);
         updateMetaParams(entry, metas);
         return null;
      }

      @Override
      public Void set(V value, Metadata metadata) {
         setValue(value);
         updateMetadata(entry, metadata);
         return null;
      }

      private void setValue(V value) {
         Object encodedValue = valueDataConversion.toStorage(value);
         entry.setValue(encodedValue);
         entry.setChanged(true);
         entry.setRemoved(value == null);
      }

      @Override
      public K key() {
         return (K) entry.getKey();
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

   private static final class EntryBackedReadWriteView<K, V> implements AccessLoggingReadWriteView<K, V> {
      final MVCCEntry entry;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;
      private final boolean existsBefore;
      private K decodedKey;
      private V decodedValue;
      private boolean isRead;

      private EntryBackedReadWriteView(MVCCEntry entry, DataConversion keyDataConversion, DataConversion valueDataConversion) {
         this.entry = entry;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
         this.existsBefore = entry.getValue() != null;
      }

      @Override
      public K key() {
         if (entry == null) {
            return null;
         }

         if (decodedKey == null) {
            decodedKey = (K) keyDataConversion.fromStorage(entry.getKey());
         }
         return decodedKey;
      }

      @Override
      public Optional<V> find() {
         isRead = true;
         return peek();
      }

      @Override
      public Optional<V> peek() {
         if (entry == null) {
            return Optional.empty();
         }
         if (decodedValue == null) {
            decodedValue = (V) valueDataConversion.fromStorage(entry.getValue());
         }
         return Optional.ofNullable(decodedValue);
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         setOnly(value, metas);
         return null;
      }

      @Override
      public Void set(V value, Metadata metadata) {
         setEntry(value);
         updateMetadata(entry, metadata);
         return null;
      }

      private void setOnly(V value, MetaParam.Writable[] metas) {
         setEntry(value);
         updateMetaParams(entry, metas);
      }

      private void setEntry(V value) {
         decodedValue = value;
         Object valueEncoded = valueDataConversion.toStorage(value);
         entry.setCreated(entry.getValue() == null && valueEncoded != null);
         entry.setValue(valueEncoded);
         entry.setChanged(true);
         entry.setRemoved(valueEncoded == null);
      }

      @Override
      public Void remove() {
         decodedValue = null;
         entry.setRemoved(existsBefore);
         entry.setChanged(existsBefore);
         entry.setValue(null);
         entry.setCreated(false);

         return null;
      }

      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         Metadata metadata = entry.getMetadata();
         if (type == MetaParam.MetaLoadedFromPersistence.class) {
            return Optional.of((T) MetaParam.MetaLoadedFromPersistence.of(entry.isLoaded()));
         }
         if (metadata instanceof MetaParamsInternalMetadata) {
            MetaParamsInternalMetadata metaParamsMetadata = (MetaParamsInternalMetadata) metadata;
            return metaParamsMetadata.findMetaParam(type);
         }

         // TODO: Add interoperability support, e.g. able to retrieve lifespan for data stored in Cache via lifespan API

         return Optional.empty();
      }

      @Override
      public V get() throws NoSuchElementException {
         isRead = true;
         if (entry == null || entry.getValue() == null)
            throw new NoSuchElementException("No value present");
         decodedValue = decodedValue == null ? (V) valueDataConversion.fromStorage(entry.getValue()) : decodedValue;
         return decodedValue;
      }

      @Override
      public String toString() {
         return "EntryBackedReadWriteView{" + "entry=" + entry + '}';
      }

      @Override
      public boolean isRead() {
         return isRead;
      }
   }

   private static final class EntryAndPreviousReadWriteView<K, V> implements AccessLoggingReadWriteView<K, V> {
      final MVCCEntry entry;
      final Object prevValue;
      final Metadata prevMetadata;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;
      private K decodedKey;
      private V decodedPrevValue;
      private V decodedValue;
      private boolean isRead;

      private EntryAndPreviousReadWriteView(MVCCEntry entry,
                                            Object prevValue,
                                            Metadata prevMetadata,
                                            DataConversion keyDataConversion,
                                            DataConversion valueDataConversion) {
         this.entry = entry;
         this.prevValue = prevValue;
         this.prevMetadata = prevMetadata;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
      }

      @Override
      public K key() {
         if (decodedKey == null) {
            decodedKey = (K) keyDataConversion.fromStorage(entry.getKey());
         }
         return decodedKey;
      }

      @Override
      public Optional<V> find() {
         isRead = true;
         return peek();
      }

      @Override
      public Optional<V> peek() {
         if (decodedPrevValue == null) {
            decodedPrevValue = (V) valueDataConversion.fromStorage(prevValue);
         }
         return Optional.ofNullable(decodedPrevValue);
      }

      public V getCurrentValue() {
         isRead = true;
         if (decodedValue == null) {
            decodedValue = (V) valueDataConversion.fromStorage(entry.getValue());
         }
         return decodedValue;
      }

      @Override
      public Void set(V value, MetaParam.Writable... metas) {
         setOnly(value, metas);
         return null;
      }

      @Override
      public Void set(V value, Metadata metadata) {
         setValue(value);
         updateMetadata(entry, metadata);
         return null;
      }

      private void setOnly(V value, MetaParam.Writable[] metas) {
         setValue(value);
         updateMetaParams(entry, metas);
      }

      private void setValue(V value) {
         decodedValue = value;
         Object valueEncoded = valueDataConversion.toStorage(value);
         entry.setValue(valueEncoded);
         entry.setChanged(true);
         entry.setRemoved(valueEncoded == null);
         entry.setCreated(prevValue == null && valueEncoded != null);
      }

      @Override
      public Void remove() {
         decodedValue = null;
         entry.setRemoved(prevValue != null);
         entry.setCreated(false);
         entry.setChanged(prevValue != null);
         entry.setValue(null);

         return null;
      }

      @Override
      public <T extends MetaParam> Optional<T> findMetaParam(Class<T> type) {
         isRead = true;
         if (type == MetaParam.MetaLoadedFromPersistence.class) {
            return Optional.of((T) MetaParam.MetaLoadedFromPersistence.of(entry.isLoaded()));
         }
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
         isRead = true;
         if (prevValue == null) throw new NoSuchElementException();
         if (decodedPrevValue == null) {
            decodedPrevValue = (V) valueDataConversion.fromStorage(prevValue);
         }
         return decodedPrevValue;
      }

      @Override
      public String toString() {
         return "EntryAndPreviousReadWriteView{" +
               "entry=" + entry +
               ", prevValue=" + prevValue +
               ", prevMetadata=" + prevMetadata +
               '}';
      }

      @Override
      public boolean isRead() {
         return isRead;
      }
   }

   private static final class NoValueReadOnlyView<K, V> implements ReadEntryView<K, V> {
      final Object key;
      private final DataConversion keyDataConversion;

      public NoValueReadOnlyView(Object key, DataConversion keyDataConversion) {
         this.key = key;
         this.keyDataConversion = keyDataConversion;
      }

      @Override
      public K key() {
         return (K) keyDataConversion.fromStorage(key);
      }

      @Override
      public V get() throws NoSuchElementException {
         throw new NoSuchElementException("No value for key " + key());
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
         return "NoValueReadOnlyView{" + "key=" + key() + '}';
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
      public Void set(V value, Metadata metadata) {
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

      updateMetadata(entry, MetaParamsInternalMetadata.from(metaParams));
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
      public void writeObject(UserObjectOutput output, ReadOnlySnapshotView object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
         output.writeObject(object.metadata);
      }

      @Override
      public ReadOnlySnapshotView readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
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
      public void writeObject(UserObjectOutput output, NoValueReadOnlyView object) throws IOException {
         output.writeObject(object.key);
      }

      @Override
      public NoValueReadOnlyView readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new NoValueReadOnlyView(input.readObject(), null);
      }
   }

   // Externalizer class defined outside of externalized class to avoid having
   // to making externalized class public, since that would leak internal impl.
   public static final class ReadWriteSnapshotViewExternalizer extends AbstractExternalizer<ReadWriteSnapshotView> {
      @Override
      public Integer getId() {
         return Ids.READ_WRITE_SNAPSHOT_VIEW;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends ReadWriteSnapshotView>> getTypeClasses() {
         return Util.asSet(ReadWriteSnapshotView.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, ReadWriteSnapshotView obj) throws IOException {
         output.writeObject(obj.key);
         output.writeObject(obj.value);
         output.writeObject(obj.metadata);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReadWriteSnapshotView readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new ReadWriteSnapshotView(key, value, metadata);
      }
   }

}
