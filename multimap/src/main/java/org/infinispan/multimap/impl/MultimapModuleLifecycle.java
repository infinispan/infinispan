package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.multimap.impl.function.hmap.HashMapKeySetFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapPutFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapRemoveFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapReplaceFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapValuesFunction;
import org.infinispan.multimap.impl.function.list.IndexFunction;
import org.infinispan.multimap.impl.function.list.IndexOfFunction;
import org.infinispan.multimap.impl.function.list.InsertFunction;
import org.infinispan.multimap.impl.function.list.OfferFunction;
import org.infinispan.multimap.impl.function.list.PollFunction;
import org.infinispan.multimap.impl.function.list.RemoveCountFunction;
import org.infinispan.multimap.impl.function.list.RotateFunction;
import org.infinispan.multimap.impl.function.list.SetFunction;
import org.infinispan.multimap.impl.function.list.SubListFunction;
import org.infinispan.multimap.impl.function.list.TrimFunction;
import org.infinispan.multimap.impl.function.multimap.ContainsFunction;
import org.infinispan.multimap.impl.function.multimap.GetFunction;
import org.infinispan.multimap.impl.function.multimap.PutFunction;
import org.infinispan.multimap.impl.function.multimap.RemoveFunction;
import org.infinispan.multimap.impl.function.set.SAddFunction;
import org.infinispan.multimap.impl.function.set.SGetFunction;
import org.infinispan.multimap.impl.function.set.SRemoveFunction;
import org.infinispan.multimap.impl.function.set.SSetFunction;
import org.infinispan.multimap.impl.function.sortedset.AddManyFunction;
import org.infinispan.multimap.impl.function.sortedset.CountFunction;
import org.infinispan.multimap.impl.function.sortedset.IncrFunction;
import org.infinispan.multimap.impl.function.sortedset.IndexOfSortedSetFunction;
import org.infinispan.multimap.impl.function.sortedset.PopFunction;
import org.infinispan.multimap.impl.function.sortedset.RemoveManyFunction;
import org.infinispan.multimap.impl.function.sortedset.ScoreFunction;
import org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction;
import org.infinispan.multimap.impl.function.sortedset.SortedSetRandomFunction;
import org.infinispan.multimap.impl.function.sortedset.SubsetFunction;

import java.util.Map;

/**
 * MultimapModuleLifecycle is necessary for the Multimap Cache module.
 * Registers advanced externalizers.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @since 9.2
 */
@InfinispanModule(name = "multimap", requiredModules = "core")
public class MultimapModuleLifecycle implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, new PersistenceContextInitializerImpl());

      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();

      addMultimapExternalizers(externalizerMap);
      addListsExternalizers(externalizerMap);
      addSetExternalizers(externalizerMap);
      addHashMapExternalizers(externalizerMap);
      addSortedSetExternalizers(externalizerMap);
   }

   /**
    * Multimap functions
    *
    * @param externalizerMap
    */
   private static void addMultimapExternalizers(Map<Integer, AdvancedExternalizer<?>> externalizerMap) {
      addAdvancedExternalizer(externalizerMap, PutFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ContainsFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, GetFunction.EXTERNALIZER);
   }

   /**
    * Lists functions
    *
    * @param externalizerMap
    */
   private static void addListsExternalizers(Map<Integer, AdvancedExternalizer<?>> externalizerMap) {
      addAdvancedExternalizer(externalizerMap, OfferFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, IndexFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, PollFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SubListFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, IndexOfFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, InsertFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveCountFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, TrimFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RotateFunction.EXTERNALIZER);
   }

   /**
    * HashMap functions
    *
    * @param externalizerMap
    */
   private static void addHashMapExternalizers(Map<Integer, AdvancedExternalizer<?>> externalizerMap) {
      addAdvancedExternalizer(externalizerMap, HashMapPutFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, HashMapKeySetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, HashMapValuesFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, HashMapRemoveFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, HashMapReplaceFunction.EXTERNALIZER);
   }

   /**
    * Set functions
    *
    * @param externalizerMap
    */
   private static void addSetExternalizers(Map<Integer, AdvancedExternalizer<?>> externalizerMap) {
      addAdvancedExternalizer(externalizerMap, SAddFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SGetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SSetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SRemoveFunction.EXTERNALIZER);
   }

   /**
    * Sorted Set functions
    *
    * @param externalizerMap
    */
   private static void addSortedSetExternalizers(Map<Integer, AdvancedExternalizer<?>> externalizerMap) {
      addAdvancedExternalizer(externalizerMap, AddManyFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, CountFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, PopFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ScoreFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SubsetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, IndexOfSortedSetFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, IncrFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SortedSetAggregateFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveManyFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, SortedSetRandomFunction.EXTERNALIZER);
   }

   private static void addAdvancedExternalizer(Map<Integer, AdvancedExternalizer<?>> map, AdvancedExternalizer<?> ext) {
      map.put(ext.getId(), ext);
   }
}
