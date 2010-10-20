package org.infinispan.loaders;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.parsing.XmlConfigHelper;
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
}

class CacheLoaderInvocationHandler implements InvocationHandler {

   private AbstractCacheStoreConfig acsc;

   public CacheLoaderInvocationHandler(AbstractCacheStoreConfig acsc) {
      super();
      this.acsc = acsc;
   }

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
         clc = instantiateCacheLoaderConfig(clClass);
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate cache loader or configuration", e);
      }
      
      clc.setCacheLoaderClassName(clClass);

      Properties props = storeConfig.getProperties();                 
      if (props != null) XmlConfigHelper.setValues(clc, props, false, true);
      
      
      if (clc instanceof CacheStoreConfig) {
         CacheStoreConfig csc = (CacheStoreConfig) clc;
         csc.setFetchPersistentState(storeConfig.isFetchPersistentState());
         csc.setIgnoreModifications(storeConfig.isIgnoreModifications());
         csc.setPurgeOnStartup(storeConfig.isPurgeOnStartup());
         csc.setPurgeSynchronously(storeConfig.isPurgeSynchronously());
         csc.setSingletonStoreConfig(storeConfig.getSingletonStoreConfig());
         csc.setAsyncStoreConfig(storeConfig.getAsyncStoreConfig());         
      }
      return clc;
   }

   private CacheLoaderConfig instantiateCacheLoaderConfig(String cacheLoaderImpl) throws Exception {
      // first see if the type is annotated
      Class<? extends CacheLoaderConfig> clazz = Util.loadClass(cacheLoaderImpl);
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