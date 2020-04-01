package org.infinispan.query.backend;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.search.analyzer.definition.LuceneAnalysisDefinitionProvider;
import org.hibernate.search.cfg.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.cfg.spi.SearchConfigurationBase;
import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.engine.service.spi.Service;
import org.hibernate.search.engine.spi.SearchMappingHelper;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.logging.Log;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.infinispan.util.logging.LogFactory;

/**
 * Class that implements {@link org.hibernate.search.cfg.spi.SearchConfiguration} so that within Infinispan-Query,
 * there is no need for a Hibernate Core configuration object.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero
 * @author anistor@redhat.com
 */
public final class SearchableCacheConfiguration extends SearchConfigurationBase implements SearchConfiguration {

   private static final String HSEARCH_PREFIX = "hibernate.search.";

   private static final Log log = LogFactory.getLog(SearchableCacheConfiguration.class, Log.class);

   private final Map<String, Class<?>> classes = new HashMap<>();
   private final Properties properties;
   private final SearchMapping searchMapping;
   private final ClassLoaderServiceImpl classLoaderService;

   public SearchableCacheConfiguration(Set<Class<?>> indexedEntities,
                                       Properties properties,
                                       Collection<ProgrammaticSearchMappingProvider> programmaticSearchMappingProviders,
                                       Collection<LuceneAnalysisDefinitionProvider> analyzerDefProviders,
                                       ComponentRegistry cr, ClassLoader aggregatedClassLoader) {
      classLoaderService = new ClassLoaderServiceImpl(aggregatedClassLoader);

      this.properties = augmentProperties(properties);

      Cache<?, ?> cache = cr.getComponent(Cache.class);

      LuceneAnalysisDefinitionProvider analyzerDefProvider = analyzerDefProviders != null && !analyzerDefProviders.isEmpty() ?
            builder -> {
               for (LuceneAnalysisDefinitionProvider provider : analyzerDefProviders) {
                  if (log.isDebugEnabled()) {
                     log.debugf("Loading LuceneAnalysisDefinitionProvider for cache %s from provider : %s", cache.getName(), provider.getClass().getName());
                  }
                  provider.register(builder);
               }
            } : null;

      //deal with programmatic mapping:
      SearchMapping searchMapping = SearchMappingHelper.extractSearchMapping(this);
      if (programmaticSearchMappingProviders != null && !programmaticSearchMappingProviders.isEmpty()) {
         if (searchMapping == null) {
            searchMapping = new SearchMapping();
         }
         for (ProgrammaticSearchMappingProvider provider : programmaticSearchMappingProviders) {
            if (log.isDebugEnabled()) {
               log.debugf("Loading programmatic search mappings for cache %s from provider : %s", cache.getName(), provider.getClass().getName());
            }
            provider.defineMappings(cache, searchMapping);
         }
      }
      this.searchMapping = searchMapping;

      for (Class<?> c : indexedEntities) {
         if (log.isDebugEnabled()) {
            log.debugf("Found configured class mapping for Hibernate Search: %s", c.getName());
         }
         classes.put(c.getName(), c);
      }

      //if we have a SearchMapping then we can predict at least those entities specified in the mapping
      //and avoid further SearchFactory rebuilds triggered by new entity discovery during cache events
      if (searchMapping != null) {
         for (Class<?> c : searchMapping.getMappedEntities()) {
            if (log.isDebugEnabled()) {
               log.debugf("Found programmatically configured class mapping for Hibernate Search: %s", c.getName());
            }
            classes.put(c.getName(), c);
         }
      }
   }

   @Override
   public boolean isDeleteByTermEnforced() {
      return true;
   }

   @Override
   public Iterator<Class<?>> getClassMappings() {
      return classes.values().iterator();
   }

   @Override
   public Class<?> getClassMapping(String name) {
      return classes.get(name);
   }

   @Override
   public String getProperty(String propertyName) {
      return properties.getProperty(propertyName);
   }

   @Override
   public Properties getProperties() {
      return properties;
   }

   @Override
   public ReflectionManager getReflectionManager() {
      return null;
   }

   @Override
   public SearchMapping getProgrammaticMapping() {
      return searchMapping;
   }

   @Override
   public Map<Class<? extends Service>, Object> getProvidedServices() {
      return Collections.emptyMap();
   }

   @Override
   public boolean isTransactionManagerExpected() {
      return false;
   }

   @Override
   public boolean isIdProvidedImplicit() {
      return true;
   }

   private Properties augmentProperties(Properties origin) {
      Properties target = new Properties();

      if (origin != null) {
         for (Entry<Object, Object> entry : origin.entrySet()) {
            Object key = entry.getKey();
            if (key instanceof String && !key.toString().startsWith(HSEARCH_PREFIX)) {
               key = HSEARCH_PREFIX + key.toString();
            }
            target.put(key, entry.getValue());
         }
      }

      // Dynamic index uninverting is now deprecated: using it will cause warnings to be logged, to encourage people to
      // use the annotation org.hibernate.search.annotations.SortableField. The default in Hibernate Search is to throw
      // an exception rather than logging a warning; we opt to be more lenient by default in the Infinispan use case,
      // matching the behaviour of previous versions of Hibernate Search, which allow dynamic sorting by default and
      // log a warning.
      target.putIfAbsent(Environment.INDEX_UNINVERTING_ALLOWED, Boolean.TRUE.toString());

      return target;
   }

   @Override
   public ClassLoaderService getClassLoaderService() {
      //FIXME wire this up to the ClassLoader configuration of the CacheManager
      //(and avoid using an .impl class from Hibernate Search)
      return classLoaderService;
   }
}
