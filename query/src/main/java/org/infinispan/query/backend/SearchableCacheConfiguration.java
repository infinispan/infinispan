package org.infinispan.query.backend;

import static org.hibernate.search.cfg.Environment.INDEX_MANAGER_IMPL_NAME;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.search.analyzer.definition.LuceneAnalysisDefinitionProvider;
import org.hibernate.search.analyzer.definition.spi.LuceneAnalysisDefinitionSourceService;
import org.hibernate.search.cfg.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.cfg.spi.SearchConfigurationBase;
import org.hibernate.search.engine.service.classloading.impl.DefaultClassLoaderService;
import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.engine.service.spi.Service;
import org.hibernate.search.engine.spi.SearchMappingHelper;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.spi.ErrorHandlerFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.affinity.AffinityErrorHandler;
import org.infinispan.query.affinity.AffinityIndexManager;
import org.infinispan.query.affinity.AffinityShardIdentifierProvider;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Class that implements {@link org.hibernate.search.cfg.spi.SearchConfiguration} so that within Infinispan-Query,
 * there
 * is
 * no need for a Hibernate Core configuration object.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero
 */
public class SearchableCacheConfiguration extends SearchConfigurationBase implements SearchConfiguration {

   private static final String HSEARCH_PREFIX = "hibernate.search.";
   private static final String SHARDING_STRATEGY = "sharding_strategy";
   private static final Log log = LogFactory.getLog(SearchableCacheConfiguration.class, Log.class);


   private final Map<String, Class<?>> classes;
   private final Properties properties;
   private final SearchMapping searchMapping;
   private final Map<Class<? extends Service>, Object> providedServices;
   private final DefaultClassLoaderService classLoaderService = new DefaultClassLoaderService();

   //TODO customize this to plug in custom analyzers
   private final LuceneAnalysisDefinitionProvider analyzerDefProvider = null;
   private boolean hasAffinity;

   public SearchableCacheConfiguration(Class<?>[] classArray, Properties properties, EmbeddedCacheManager uninitializedCacheManager, ComponentRegistry cr) {
      this.providedServices = initializeProvidedServices(uninitializedCacheManager, cr);
      if (properties == null) {
         this.properties = new Properties();
      } else {
         this.properties = augment(properties);
      }

      classes = new HashMap<>();

      for (Class<?> c : classArray) {
         String classname = c.getName();
         classes.put(classname, c);
      }

      if (hasAffinity) {
         ErrorHandler configuredErrorHandler = ErrorHandlerFactory.createErrorHandler(this);
         this.properties.put(Environment.ERROR_HANDLER, new AffinityErrorHandler(configuredErrorHandler));
      }

      //deal with programmatic mapping:
      searchMapping = SearchMappingHelper.extractSearchMapping(this);

      //if we have a SearchMapping then we can predict at least those entities specified in the mapping
      //and avoid further SearchFactory rebuilds triggered by new entity discovery during cache events
      if (searchMapping != null) {
         Set<Class<?>> mappedEntities = searchMapping.getMappedEntities();
         for (Class<?> entity : mappedEntities) {
            classes.put(entity.getName(), entity);
         }
      }
   }

   private Map initializeProvidedServices(EmbeddedCacheManager uninitializedCacheManager, ComponentRegistry cr) {
      //Register the SelfLoopedCacheManagerServiceProvider to allow custom IndexManagers to access the CacheManager
      final InfinispanLoopbackService loopService = new InfinispanLoopbackService(cr, uninitializedCacheManager);
      HashMap map = new HashMap(3);
      map.put(LuceneAnalysisDefinitionSourceService.class, new LuceneAnalyzerDefinitionsBuilderService(analyzerDefProvider));
      map.put(ComponentRegistryService.class, loopService);
      map.put(CacheManagerService.class, loopService);
      return Collections.unmodifiableMap(map);
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
      return providedServices;
   }

   @Override
   public boolean isTransactionManagerExpected() {
      return false;
   }

   @Override
   public boolean isIdProvidedImplicit() {
      return true;
   }

   private Properties augment(Properties origin) {
      Properties target = new Properties();
      for (Entry<Object, Object> entry : origin.entrySet()) {
         Object key = entry.getKey();
         if (key instanceof String && !key.toString().startsWith(HSEARCH_PREFIX)) {
            key = HSEARCH_PREFIX + key.toString();
         }
         target.put(key, entry.getValue());
         if (key.toString().endsWith(INDEX_MANAGER_IMPL_NAME) && entry.getValue().equals(AffinityIndexManager.class.getName())) {
            target.put(key.toString().replace(INDEX_MANAGER_IMPL_NAME, SHARDING_STRATEGY), AffinityShardIdentifierProvider.class.getName());
            hasAffinity = true;
         }
      }
      return target;
   }

   @Override
   public ClassLoaderService getClassLoaderService() {
      //FIXME wire this up to the ClassLoader configuration of the CacheManager
      //(and avoid using an .impl class from Hibernate Search)
      return classLoaderService;
   }

}
