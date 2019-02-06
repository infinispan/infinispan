package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Immutables;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.persistence.impl.MarshalledEntryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.tools.store.migrator.marshaller.common.InternalMetadataImplExternalizer;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * The externalizer table maintains information necessary to be able to map a particular type with the corresponding
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementation that it marshall, and it also keeps
 * information of which {@link org.infinispan.commons.marshall.AdvancedExternalizer} should be used to read data from a
 * buffer given a particular {@link org.infinispan.commons.marshall.AdvancedExternalizer} identifier.
 * <p>
 * These tables govern how either internal Infinispan classes, or user defined classes, are marshalled to a given
 * output, or how these are unmarshalled from a given input.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class ExternalizerTable implements ObjectTable {
   private static final Map<Integer, Integer> LEGACY_MAP;
   static {
      HashMap<Integer, Integer> map = new HashMap<>();
      map.put(2, 1); // MAPS
      map.put(10, 7); // IMMORTAL_ENTRY
      map.put(11, 8); // MORTAL_ENTRY
      map.put(12, 9); // TRANSIENT_ENTRY
      map.put(13, 10); // TRANSIENT_MORTAL_ENTRY
      map.put(14, 11); // IMMORTAL_VALUE
      map.put(15, 12); // MORTAL_VALUE
      map.put(16, 13); // TRANSIENT_VALUE
      map.put(17, 14); // TRANSIENT_VALUE
      map.put(19, 105); // IMMUTABLE_MAP
      map.put(76, 38); // METADATA_IMMORTAL_ENTRY
      map.put(77, 39); // METADATA_MORTAL_ENTRY
      map.put(78, 40); // METADATA_TRANSIENT_ENTRY
      map.put(79, 41); // METADATA_TRANSIENT_MORTAL_ENTRY
      map.put(80, 42); // METADATA_IMMORTAL_ENTRY
      map.put(81, 43); // METADATA_MORTAL_ENTRY
      map.put(82, 44); // METADATA_TRANSIENT_VALUE
      map.put(83, 45); // METADATA_TRANSIENT_MORTAL_VALUE
      map.put(96, 55); // SIMPLE_CLUSTERED_VERSION
      map.put(98, 57); // EMBEDDED_METADATA
      map.put(99, 58); // NUMERIC_VERSION
      map.put(103, 60); // KEY_VALUE_PAIR
      map.put(105, 62); // MARSHALLED_ENTRY
      map.put(106, 106); // BYTE_BUFFER
      map.put(121, 63); // ENUM_SET
      LEGACY_MAP = Collections.unmodifiableMap(map);
   }

   static int ARRAY_LIST = 0;
   static int JDK_SETS = 3;
   static int SINGLETON_LIST = 4;
   static int IMMUTABLE_LIST = 18;
   static int INTERNAL_METADATA = 104;
   static int LIST_ARRAY = 122;

   private static final Log log = LogFactory.getLog(ExternalizerTable.class);
   private static final int MAX_ID = 255;

   private final Map<Integer, ExternalizerAdapter> readers = new HashMap<>();
   private final StreamingMarshaller marshaller;

   ExternalizerTable(StreamingMarshaller marshaller, Map<Integer, ? extends AdvancedExternalizer> externalizerMap) {
      this.marshaller = marshaller;
      loadInternalMarshallables();
      initForeignMarshallables(externalizerMap);
   }

   @Override
   public Writer getObjectWriter(Object o) {
      return null;
   }

   @Override
   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int readerIndex = input.readUnsignedByte();

      int foreignId = -1;
      if (readerIndex == MAX_ID) {
         // User defined externalizer
         foreignId = UnsignedNumeric.readUnsignedInt(input);
         readerIndex = generateForeignReaderIndex(foreignId);
      } else {
         Integer legacyId = LEGACY_MAP.get(readerIndex);
         if (legacyId != null)
            readerIndex = legacyId;
      }

      ExternalizerAdapter adapter = readers.get(readerIndex);
      if (adapter == null) {
         if (foreignId > 0)
            throw log.missingForeignExternalizer(foreignId);

         throw log.unknownExternalizerReaderIndex(readerIndex);
      }
      return adapter.externalizer.readObject(input);
   }

   private void loadInternalMarshallables() {
      addInternalExternalizer(new ListExternalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new EnumSetExternalizer());
      addInternalExternalizer(new ArrayExternalizers.ListArray());
      addInternalExternalizer(new SingletonListExternalizer());

      addInternalExternalizer(new ImmutableListCopyExternalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new ByteBufferImpl.Externalizer());

      addInternalExternalizer(new NumericVersion.Externalizer());
      addInternalExternalizer(new ByteBufferImpl.Externalizer());
      addInternalExternalizer(new KeyValuePair.Externalizer());
      addInternalExternalizer(new InternalMetadataImplExternalizer(INTERNAL_METADATA));
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller));

      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new SimpleClusteredVersion.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataTransientCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataTransientCacheValue.Externalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new EmbeddedMetadata.Externalizer());
   }

   private void addInternalExternalizer(AdvancedExternalizer ext) {
      int id = checkInternalIdLimit(ext.getId(), ext);
      updateExtReadersWithTypes(new ExternalizerAdapter(id, ext));
   }

   private void updateExtReadersWithTypes(ExternalizerAdapter adapter) {
      updateExtReadersWithTypes(adapter, adapter.id);
   }

   private void updateExtReadersWithTypes(ExternalizerAdapter adapter, int readerIndex) {
      Set<Class<?>> typeClasses = adapter.externalizer.getTypeClasses();
      if (typeClasses.size() > 0) {
         for (Class<?> typeClass : typeClasses)
            updateExtReaders(adapter, typeClass, readerIndex);
      } else {
         throw log.advanceExternalizerTypeClassesUndefined(adapter.externalizer.getClass().getName());
      }
   }

   private void initForeignMarshallables(Map<Integer, ? extends AdvancedExternalizer> externalizerMap) {
      for (Map.Entry<Integer, ? extends AdvancedExternalizer> entry : externalizerMap.entrySet()) {
         AdvancedExternalizer ext = entry.getValue();
         Integer id = ext.getId();
         if (entry.getKey() == null && id == null)
            throw new CacheConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (entry.getKey() != null)
            id = entry.getKey();

         id = checkForeignIdLimit(id, ext);
         updateExtReadersWithTypes(new ExternalizerAdapter(id, ext), generateForeignReaderIndex(id));
      }
   }

   private void updateExtReaders(ExternalizerAdapter adapter, Class<?> typeClass, int readerIndex) {
      ExternalizerAdapter prevReader = readers.put(readerIndex, adapter);
      // Several externalizers might share same id (i.e. HashMap and TreeMap use MapExternalizer)
      // but a duplicate is only considered when that particular index has already been entered
      // in the readers map and the externalizers are different (they're from different classes)
      if (prevReader != null && !prevReader.equals(adapter))
         throw log.duplicateExternalizerIdFound(adapter.id, typeClass, prevReader.externalizer.getClass().getName(), readerIndex);
   }

   private int checkInternalIdLimit(int id, AdvancedExternalizer ext) {
      if (id >= MAX_ID)
         throw log.internalExternalizerIdLimitExceeded(ext, id, MAX_ID);
      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer ext) {
      if (id < 0)
         throw log.foreignExternalizerUsingNegativeId(ext, id);
      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   private static class ExternalizerAdapter {
      final int id;
      final AdvancedExternalizer externalizer;

      ExternalizerAdapter(int id, AdvancedExternalizer externalizer) {
         this.id = id;
         this.externalizer = externalizer;
      }

      @Override
      public String toString() {
         // Each adapter is represented by the externalizer it delegates to, so just return the class name
         return externalizer.getClass().getName();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExternalizerAdapter that = (ExternalizerAdapter) o;
         if (id != that.id) return false;
         if (externalizer != null ? !externalizer.getClass().equals(that.externalizer.getClass()) : that.externalizer != null)
            return false;
         return true;
      }

      @Override
      public int hashCode() {
         int result = id;
         result = 31 * result + (externalizer.getClass() != null ? externalizer.getClass().hashCode() : 0);
         return result;
      }
   }
}
