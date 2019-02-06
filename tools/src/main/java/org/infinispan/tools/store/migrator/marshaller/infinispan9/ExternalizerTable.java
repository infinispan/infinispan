package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
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
import org.infinispan.marshall.exts.CollectionExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.persistence.impl.MarshalledEntryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.tools.store.migrator.marshaller.common.InternalMetadataImplExternalizer;
import org.infinispan.util.KeyValuePair;

/**
 * Legacy externalizers for Infinispan 9.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
class ExternalizerTable {

   private static final int INTERNAL_METADATA = 61;
   private static final Set<AdvancedExternalizer> INTERNAL_EXTERNALIZERS = new HashSet<>(26);
   static {
      INTERNAL_EXTERNALIZERS.add(new CollectionExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MapExternalizer());
      INTERNAL_EXTERNALIZERS.add(new EnumSetExternalizer());
      INTERNAL_EXTERNALIZERS.add(new Immutables.ImmutableMapWrapperExternalizer());
      INTERNAL_EXTERNALIZERS.add(new ByteBufferImpl.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new NumericVersion.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new KeyValuePair.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new InternalMetadataImplExternalizer(INTERNAL_METADATA));
      INTERNAL_EXTERNALIZERS.add(new ImmortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientMortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new ImmortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientMortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new SimpleClusteredVersion.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataImmortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataMortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientMortalCacheEntry.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataImmortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataMortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientMortalCacheValue.Externalizer());
      INTERNAL_EXTERNALIZERS.add(new EmbeddedMetadata.Externalizer());
   }

   static  Map<Integer, AdvancedExternalizer> getInternalExternalizers(Marshaller marshaller) {
      Set<AdvancedExternalizer> initializedExternalizers = new HashSet<>(INTERNAL_EXTERNALIZERS);
      initializedExternalizers.add(new MarshalledEntryImpl.Externalizer(marshaller));
      return initializedExternalizers.stream().collect(Collectors.toMap(AdvancedExternalizer::getId, Function.identity()));
   }
}
