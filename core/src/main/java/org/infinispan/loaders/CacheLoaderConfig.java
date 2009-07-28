package org.infinispan.loaders;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Properties;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.infinispan.config.ConfigurationException;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.util.Util;

/**
 * Configures individual cache loaders
 *
 * @author Manik Surtani
 * @since 4.0
 */
@XmlJavaTypeAdapter(CacheLoaderConfigAdapter.class)
public interface CacheLoaderConfig extends Cloneable {

   CacheLoaderConfig clone();

   String getCacheLoaderClassName();

   void setCacheLoaderClassName(String s);
}

class CacheLoaderInvocationhandler implements InvocationHandler {

   private AbstractCacheStoreConfig acsc;

   public CacheLoaderInvocationhandler(AbstractCacheStoreConfig acsc) {
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
      
      CacheLoader cl;
      CacheLoaderConfig clc;
      try {
         cl = (CacheLoader) Util.getInstance(clClass);
         clc = Util.getInstance(cl.getConfigurationClass());
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
   }}