package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.QueryCache;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.util.KeyValuePair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPAContinuousQueryProtobufCacheEventFilterConverter extends AbstractCacheEventFilterConverter<Object, Object, byte[]> {

   /**
    * The JPA query to execute.
    */
   private final String jpaQuery;

   /**
    * The implementation class of the Matcher component to lookup and use.
    */
   private final Class<? extends Matcher> matcherImplClass;

   private transient SerializationContext serCtx;

   /**
    * Optional cache for query objects.
    */
   private transient QueryCache queryCache;

   /**
    * The Matcher, acquired via dependency injection.
    */
   private transient Matcher matcher;

   /**
    * The ObjectFilter is created lazily.
    */
   private transient ObjectFilter objectFilter;

   private boolean usesValueWrapper;

   public JPAContinuousQueryProtobufCacheEventFilterConverter(String jpaQuery, Class<? extends Matcher> matcherImplClass) {
      if (jpaQuery == null || matcherImplClass == null) {
         throw new IllegalArgumentException("Arguments cannot be null");
      }
      this.jpaQuery = jpaQuery;
      this.matcherImplClass = matcherImplClass;
   }

   @Inject
   protected void injectDependencies(Cache cache) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      queryCache = cache.getCacheManager().getGlobalComponentRegistry().getComponent(QueryCache.class);
      matcher = cache.getAdvancedCache().getComponentRegistry().getComponent(matcherImplClass);
      if (matcher == null) {
         throw new CacheException("Expected component not found in registry: " + matcherImplClass.getName());
      }
      Configuration cfg = cache.getCacheConfiguration();
      usesValueWrapper = cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled();
   }

   private ObjectFilter getObjectFilter() {
      if (objectFilter == null) {
         if (queryCache != null) {
            KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpaQuery, matcherImplClass);
            ObjectFilter objectFilter = queryCache.get(queryCacheKey);
            if (objectFilter == null) {
               objectFilter = matcher.getObjectFilter(jpaQuery);
               queryCache.put(queryCacheKey, objectFilter);
            }
            this.objectFilter = objectFilter;
         } else {
            objectFilter = matcher.getObjectFilter(jpaQuery);
         }
      }
      return objectFilter;
   }

   private ObjectFilter.FilterResult filterAndConvert(Object value) {
      if (value == null) {
         return null;
      }
      return getObjectFilter().filter(value);
   }

   private ContinuousQueryResult filterAndConvert(Object key, Object oldValue, Object newValue, EventType eventType) {
      ObjectFilter.FilterResult f1 = filterAndConvert(oldValue);
      ObjectFilter.FilterResult f2 = filterAndConvert(newValue);

      if (f2 != null && eventType.isExpired()) {  // expired events return expired value as newValue
         return new ContinuousQueryResult(false, (byte[]) key, null);
      }
      
      if (f1 == null && f2 != null) {
         return new ContinuousQueryResult(true, (byte[]) key, (byte[]) newValue);
      }

      if (f1 != null && f2 == null) {
         return new ContinuousQueryResult(false, (byte[]) key, null);
      }

      return null;
   }

   @Override
   public byte[] filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      if (usesValueWrapper) {
         if (oldValue instanceof ProtobufValueWrapper) {
            oldValue = ((ProtobufValueWrapper) oldValue).getBinary();
         }
         if (newValue instanceof ProtobufValueWrapper) {
            newValue = ((ProtobufValueWrapper) newValue).getBinary();
         }
      }
      ContinuousQueryResult continuousQueryResult = filterAndConvert(key, oldValue, newValue, eventType);
      if (continuousQueryResult != null) {
         try {
            return ProtobufUtil.toByteArray(serCtx, continuousQueryResult);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      return null;
   }

   public static final class Externalizer extends AbstractExternalizer<JPAContinuousQueryProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAContinuousQueryProtobufCacheEventFilterConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.jpaQuery);
         output.writeObject(filterAndConverter.matcherImplClass);
      }

      @Override
      public JPAContinuousQueryProtobufCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String jpaQuery = input.readUTF();
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new JPAContinuousQueryProtobufCacheEventFilterConverter(jpaQuery, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAContinuousQueryProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.<Class<? extends JPAContinuousQueryProtobufCacheEventFilterConverter>>singleton(JPAContinuousQueryProtobufCacheEventFilterConverter.class);
      }
   }
}
