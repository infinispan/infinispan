package org.infinispan.loaders;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Configures individual cache loaders
 * 
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlJavaTypeAdapter(CacheLoaderConfigAdapter.class)
public interface CacheLoaderConfig extends Cloneable, Serializable {
   
   void accept(ConfigurationBeanVisitor visitor);

   CacheLoaderConfig clone();

   String getCacheLoaderClassName();

   void setCacheLoaderClassName(String s);
   
   /**
    * Get the classloader that should be used to load resources from the classpath
    */
   ClassLoader getClassLoader();
}

class CacheLoaderInvocationHandler implements InvocationHandler {

   private AbstractCacheStoreConfig acsc;

   public CacheLoaderInvocationHandler(AbstractCacheStoreConfig acsc) {
      super();
      this.acsc = acsc;
   }

   @Override
   public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return method.invoke(acsc, args);
   }
}

class CacheLoaderConfigAdapter extends XmlAdapter<AbstractCacheStoreConfig, CacheLoaderConfig>{

   @Override
   public AbstractCacheStoreConfig marshal(CacheLoaderConfig arg0) throws Exception {         
      return (AbstractCacheStoreConfig) arg0;
   }

   @Override
   public CacheLoaderConfig unmarshal(AbstractCacheStoreConfig storeConfig) throws Exception {
      String clClass = storeConfig.getCacheLoaderClassName();
      if (clClass == null || clClass.length()==0)
         throw new ConfigurationException("Missing 'class'  attribute for cache loader configuration");

      CacheLoaderConfig clc;
      try {
         clc = instantiateCacheLoaderConfig(clClass, storeConfig.getClassLoader());
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate cache loader or configuration", e);
      }
      
      clc.setCacheLoaderClassName(clClass);

      Properties props = storeConfig.getProperties();                 
      if (props != null) XmlConfigHelper.setValues(clc, props, false, true);
      
      
      if (clc instanceof CacheStoreConfig) {
         CacheStoreConfig csc = (CacheStoreConfig) clc;
         csc
               .fetchPersistentState(storeConfig.isFetchPersistentState())
               .ignoreModifications(storeConfig.isIgnoreModifications())
               .purgeOnStartup(storeConfig.isPurgeOnStartup())
               .purgeSynchronously(storeConfig.isPurgeSynchronously())
               .purgerThreads(storeConfig.getPurgerThreads());

         SingletonStoreConfig singletonStoreConfig = storeConfig.getSingletonStoreConfig();
         if (singletonStoreConfig != null) {
            csc.singletonStore()
               .enabled(singletonStoreConfig.isSingletonStoreEnabled())
               .pushStateTimeout(singletonStoreConfig.getPushStateTimeout());
         }

         AsyncStoreConfig asyncStoreConfig = storeConfig.getAsyncStoreConfig();
         if (asyncStoreConfig != null && asyncStoreConfig.isEnabled()) {
            csc.asyncStore()
                  .flushLockTimeout(asyncStoreConfig.getFlushLockTimeout())
                  .shutdownTimeout(asyncStoreConfig.getShutdownTimeout())
                  .threadPoolSize(asyncStoreConfig.getThreadPoolSize())
                  .modificationQueueSize(asyncStoreConfig.getModificationQueueSize());
         }
      }
      return clc;
   }

   private CacheLoaderConfig instantiateCacheLoaderConfig(String cacheLoaderImpl, ClassLoader classLoader) throws Exception {
      // first see if the type is annotated
      Class<? extends CacheLoaderConfig> clazz = Util.loadClass(cacheLoaderImpl, classLoader);
      Class<? extends CacheLoaderConfig> cacheLoaderConfigType;
      CacheLoaderMetadata metadata = clazz.getAnnotation(CacheLoaderMetadata.class);
      if (metadata == null) {
         CacheLoader cl = (CacheLoader) Util.getInstance(clazz);
         cacheLoaderConfigType = cl.getConfigurationClass();
      } else {
         cacheLoaderConfigType = metadata.configurationClass();
      }
      return Util.getInstance(cacheLoaderConfigType);
   }
}