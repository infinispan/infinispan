package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.UpdateLuceneWork;
import org.hibernate.search.bridge.spi.ConversionContext;
import org.hibernate.search.bridge.util.impl.ContextualExceptionBridgeHelper;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.impl.SimpleInitializer;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public final class IndexingMapper implements Mapper<Object, Object, Object, LuceneWork> {

   private AdvancedCache cache;
   private SearchFactoryIntegrator searchFactory;
   private QueryInterceptor queryInterceptor;
   private KeyTransformationHandler keyTransformationHandler;

   public void initialize(Cache inputCache) {
      this.cache = inputCache.getAdvancedCache();
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      this.searchFactory = queryInterceptor.getSearchFactory();
      this.keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
   }

   @Override
   public void map(Object key, Object value, Collector<Object, LuceneWork> collector) {
      if (queryInterceptor.updateKnownTypesIfNeeded(value)) {
         updateIndex(key, value, collector);
      }
   }

   private void updateIndex(Object key, Object value, Collector<Object, LuceneWork> collector) {
      Class clazz = value.getClass();
      EntityIndexBinding entityIndexBinding = searchFactory.getIndexBinding(clazz);
      if (entityIndexBinding == null) {
         // it might be possible to receive not-indexes types
         return;
      }
      ConversionContext conversionContext = new ContextualExceptionBridgeHelper();
      DocumentBuilderIndexedEntity docBuilder = entityIndexBinding.getDocumentBuilder();
      final String idInString = keyTransformationHandler.keyToString(key);
      UpdateLuceneWork updateTask = docBuilder.createUpdateWork(
            clazz,
            value,
            idInString,
            idInString,
            SimpleInitializer.INSTANCE,
            conversionContext
      );
      collector.emit(key, updateTask);
   }

}
