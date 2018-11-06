package org.infinispan.tools.store.migrator.marshaller;

import java.io.IOException;
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
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.tools.store.migrator.marshaller.externalizers.ArrayExternalizers;
import org.infinispan.tools.store.migrator.marshaller.externalizers.ImmutableListCopyExternalizer;
import org.infinispan.tools.store.migrator.marshaller.externalizers.LegacyIds;
import org.infinispan.tools.store.migrator.marshaller.externalizers.ListExternalizer;
import org.infinispan.tools.store.migrator.marshaller.externalizers.SetExternalizer;
import org.infinispan.tools.store.migrator.marshaller.externalizers.SingletonListExternalizer;
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
   private static final Log log = LogFactory.getLog(ExternalizerTable.class);
   private static final int MAX_ID = 255;

   private final Map<Integer, ExternalizerAdapter> readers = new HashMap<>();
   private final StreamingMarshaller globalMarshaller;

   ExternalizerTable(StreamingMarshaller globalMarshaller, Map<Integer, ? extends AdvancedExternalizer<?>> externalizerMap) {
      this.globalMarshaller = globalMarshaller;
      loadInternalMarshallables();
      initForeignMarshallables(externalizerMap);
   }

   @Override
   public Writer getObjectWriter(Object o) throws IOException {
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
         Integer legacyId = LegacyIds.LEGACY_MAP.get(readerIndex);
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
      addInternalExternalizer(new InternalMetadataImpl.Externalizer());
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(globalMarshaller));

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

   private void addInternalExternalizer(AdvancedExternalizer<?> ext) {
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

   private void initForeignMarshallables(Map<Integer, ? extends AdvancedExternalizer<?>> externalizerMap) {
      for (Map.Entry<Integer, ? extends AdvancedExternalizer<?>> entry : externalizerMap.entrySet()) {
         AdvancedExternalizer<?> ext = entry.getValue();
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

   private int checkInternalIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id >= MAX_ID)
         throw log.internalExternalizerIdLimitExceeded(ext, id, MAX_ID);
      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id < 0)
         throw log.foreignExternalizerUsingNegativeId(ext, id);

      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   private static class ExternalizerAdapter {
      final int id;
      final AdvancedExternalizer<Object> externalizer;

      ExternalizerAdapter(int id, AdvancedExternalizer<?> externalizer) {
         this.id = id;
         this.externalizer = (AdvancedExternalizer<Object>) externalizer;
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
