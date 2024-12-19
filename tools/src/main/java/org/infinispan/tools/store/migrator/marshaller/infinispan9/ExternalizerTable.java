package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
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
      INTERNAL_EXTERNALIZERS.add(new ImmutableMapWrapperExternalizer());
      INTERNAL_EXTERNALIZERS.add(new ByteBufferImplExternalizer());
      INTERNAL_EXTERNALIZERS.add(new NumericVersionExternalizer());
      INTERNAL_EXTERNALIZERS.add(new KeyValuePairExternalizer());
      INTERNAL_EXTERNALIZERS.add(new InternalMetadataImplExternalizer(INTERNAL_METADATA));
      INTERNAL_EXTERNALIZERS.add(new ImmortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientMortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new ImmortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new TransientMortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new SimpleClusteredVersionExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataImmortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataMortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientMortalCacheEntryExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataImmortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataMortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new MetadataTransientMortalCacheValueExternalizer());
      INTERNAL_EXTERNALIZERS.add(new EmbeddedMetadataExternalizer());
   }

   static  Map<Integer, AdvancedExternalizer> getInternalExternalizers(Marshaller marshaller) {
      Set<AdvancedExternalizer> initializedExternalizers = new HashSet<>(INTERNAL_EXTERNALIZERS);
      initializedExternalizers.add(new MarshalledEntryImpl.Externalizer(marshaller));
      return initializedExternalizers.stream().collect(Collectors.toMap(AdvancedExternalizer::getId, Function.identity()));
   }
}
