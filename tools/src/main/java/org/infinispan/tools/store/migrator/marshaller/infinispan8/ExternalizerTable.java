package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.jboss.marshalling.commons.StreamingMarshaller;
import org.infinispan.tools.store.migrator.marshaller.common.ByteBufferImplExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.EmbeddedMetadataExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.EnumSetExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.ImmortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.ImmortalCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.ImmutableMapWrapperExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.InternalMetadataImplExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.KeyValuePairExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MapExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MarshalledEntryImpl;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataImmortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataImmortalCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataMortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataMortalCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataTransientCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataTransientCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataTransientMortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MetadataTransientMortalCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.MortalCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.NumericVersionExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.SimpleClusteredVersionExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.TransientCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.TransientCacheValueExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.TransientMortalCacheEntryExternalizer;
import org.infinispan.tools.store.migrator.marshaller.common.TransientMortalCacheValueExternalizer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * The externalizer table maintains information necessary to be able to map a particular type with the corresponding
 * {@link AdvancedExternalizer} implementation that it marshall, and it also keeps
 * information of which {@link AdvancedExternalizer} should be used to read data from a
 * buffer given a particular {@link AdvancedExternalizer} identifier.
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
      // ENUM_SET
      LEGACY_MAP = Map.ofEntries(Map.entry(2, 1), // MAPS
            Map.entry(10, 7), // IMMORTAL_ENTRY
            Map.entry(11, 8), // MORTAL_ENTRY
            Map.entry(12, 9), // TRANSIENT_ENTRY
            Map.entry(13, 10), // TRANSIENT_MORTAL_ENTRY
            Map.entry(14, 11), // IMMORTAL_VALUE
            Map.entry(15, 12), // MORTAL_VALUE
            Map.entry(16, 13), // TRANSIENT_VALUE
            Map.entry(17, 14), // TRANSIENT_VALUE
            Map.entry(19, 105), // IMMUTABLE_MAP
            Map.entry(76, 38), // METADATA_IMMORTAL_ENTRY
            Map.entry(77, 39), // METADATA_MORTAL_ENTRY
            Map.entry(78, 40), // METADATA_TRANSIENT_ENTRY
            Map.entry(79, 41), // METADATA_TRANSIENT_MORTAL_ENTRY
            Map.entry(80, 42), // METADATA_IMMORTAL_ENTRY
            Map.entry(81, 43), // METADATA_MORTAL_ENTRY
            Map.entry(82, 44), // METADATA_TRANSIENT_VALUE
            Map.entry(83, 45), // METADATA_TRANSIENT_MORTAL_VALUE
            Map.entry(96, 55), // SIMPLE_CLUSTERED_VERSION
            Map.entry(98, 57), // EMBEDDED_METADATA
            Map.entry(99, 58), // NUMERIC_VERSION
            Map.entry(103, 60), // KEY_VALUE_PAIR
            Map.entry(105, 62), // MARSHALLED_ENTRY
            Map.entry(106, 106), // BYTE_BUFFER
            Map.entry(121, 63));
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
      addInternalExternalizer(new ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new ByteBufferImplExternalizer());

      addInternalExternalizer(new NumericVersionExternalizer());
      addInternalExternalizer(new KeyValuePairExternalizer());
      addInternalExternalizer(new InternalMetadataImplExternalizer(INTERNAL_METADATA));
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller));

      addInternalExternalizer(new ImmortalCacheEntryExternalizer());
      addInternalExternalizer(new MortalCacheEntryExternalizer());
      addInternalExternalizer(new TransientCacheEntryExternalizer());
      addInternalExternalizer(new TransientMortalCacheEntryExternalizer());
      addInternalExternalizer(new ImmortalCacheValueExternalizer());
      addInternalExternalizer(new MortalCacheValueExternalizer());
      addInternalExternalizer(new TransientCacheValueExternalizer());
      addInternalExternalizer(new TransientMortalCacheValueExternalizer());

      addInternalExternalizer(new SimpleClusteredVersionExternalizer());
      addInternalExternalizer(new MetadataImmortalCacheEntryExternalizer());
      addInternalExternalizer(new MetadataMortalCacheEntryExternalizer());
      addInternalExternalizer(new MetadataTransientCacheEntryExternalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheEntryExternalizer());
      addInternalExternalizer(new MetadataImmortalCacheValueExternalizer());
      addInternalExternalizer(new MetadataMortalCacheValueExternalizer());
      addInternalExternalizer(new MetadataTransientCacheValueExternalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheValueExternalizer());

      addInternalExternalizer(new EmbeddedMetadataExternalizer());
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
      if (!typeClasses.isEmpty()) {
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
      if (id >= MAX_ID) {
         String msg = String.format("Internal %s externalizer is using an id(%d) that exceeded the limit. It needs to be smaller than %d", ext.getClass().getSimpleName(), id, MAX_ID);
         throw new CacheConfigurationException(msg);
      }
      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer ext) {
      if (id < 0) {
         String msg = String.format("Foreign %s externalizer is using a negative id(%d). Only positive id values are allowed.", ext.getClass().getSimpleName(), id);
         throw new CacheConfigurationException(msg);
      }
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
         if (externalizer != null ? externalizer.getClass() != that.externalizer.getClass() : that.externalizer != null)
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
