/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.backend;

import org.hibernate.search.cfg.SearchConfiguration;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.impl.SearchFactoryImpl;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.InterceptorChainFactory;
import org.infinispan.interceptors.DistLockingInterceptor;
import org.infinispan.interceptors.LockingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

/**
 * <p/>
 * This is a TEMPORARY helper class that will be used to add the QueryInterceptor to the chain and provide Classes to
 * Hibernate Search.
 * <p/>
 * This class needs to be instantiated before adding any objects into the Cache. Any objects added before this
 * instantiation will not be indexed.
 * <p/>
 * This class must be instantiated only once however.
 * <p/>
 * However, only one instan This class WILL be removed once other hooks come into Infinispan for versions 4.1 etc.
 *
 * @author Navin Surtani
 * @since 4.0
 */
public class QueryHelper {

   private Cache cache;
   private Properties properties;
   private Class[] classes;
   private SearchFactoryImplementor searchFactory;

   Log log = LogFactory.getLog(getClass());

   /**
    * Constructor that will take in 3 params and build the searchFactory for Hibernate Search.
    * <p/>
    * <p/>
    * Once this constructor is called, the user MUST call applyProperties() to set up the interceptors.
    * <p/>
    *
    * @param cache      - the cache instance.
    * @param properties - {@link java.util.Properties}
    * @param classes    - the Class[] for Hibernate Search.
    */

   public QueryHelper(Cache cache, Properties properties, Class... classes) {
      // assume cache is already created and running.
      // otherwise, start the cache!!
      if (cache.getStatus().needToInitializeBeforeStart()) {
         if(log.isDebugEnabled()) log.debug("Cache not started.  Starting cache first.");
         cache.start();
      }

      checkInterceptorChain(cache);

      if (classes.length == 0) {
         throw new IllegalArgumentException("You haven't passed in any classes to index.");
      }

      // Make the validation check here.
      validateClasses(classes);

      if (properties == null) log.debug("Properties is null.");

      this.cache = cache;
      this.properties = properties;
      this.classes = classes;

      // Set up the search factory for hibernate search first.

      SearchConfiguration cfg = new SearchableCacheConfiguration(classes, properties);
      searchFactory = new SearchFactoryImpl(cfg);

      applyProperties(cache.getConfiguration());
   }

   /**
    * This method MUST be called if running the query module and you want to index objects in the cache.
    * <p/>
    * <p/>
    * <p/>
    * e.g.:- QueryHelper.applyQueryProperties(); has to be used BEFORE any objects are put in the cache so that they can
    * be indexed.
    * <p/>
    * <p/>
    * <p/>
    * <p/>
    * Anything put before calling this method will NOT not be picked up by the {@link QueryInterceptor} and hence not be
    * indexed.
    */

   private void applyProperties(Configuration cfg) {
      if (log.isDebugEnabled()) log.debug("Entered QueryHelper.applyProperties()");

      if (cfg.isIndexingEnabled()) {

         try {
            if (cfg.isIndexLocalOnly()) {
               // Add a LocalQueryInterceptor to the chain
               initComponents(cfg, LocalQueryInterceptor.class);
            } else {
               // We're indexing data even if it comes from other sources
               // Add in a QueryInterceptor to the chain
               initComponents(cfg, QueryInterceptor.class);
            }
         } catch (Exception e) {
            throw new CacheException("Unable to add interceptor", e);
         }
      }
   }

   /**
    * Simple getter.
    *
    * @return the {@link org.hibernate.search.engine.SearchFactoryImplementor} instance being used.
    */

   public SearchFactoryImplementor getSearchFactory() {
      return searchFactory;
   }

   /**
    * Simple getter.
    *
    * @return the class[].
    */


   public Class[] getClasses() {
      return classes;
   }

   /**
    * Simple getter.
    *
    * @return {@link java.util.Properties}
    */

   public Properties getProperties() {
      return properties;
   }


   // Private method that adds the interceptor from the classname parameter.


   private void initComponents(Configuration cfg, Class<? extends QueryInterceptor> interceptorClass)
         throws IllegalAccessException, InstantiationException {

      // get the component registry and then register the searchFactory.
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      cr.registerComponent(searchFactory, SearchFactoryImplementor.class);
      cr.registerComponent(new SearchFactoryStopper(searchFactory), SearchFactoryStopper.class);

      // Get the interceptor chain factory so I can create my interceptor.
      InterceptorChainFactory icf = cr.getComponent(InterceptorChainFactory.class);

      CommandInterceptor inter = icf.createInterceptor(interceptorClass);
      cr.registerComponent(inter, QueryInterceptor.class);

      cache.getAdvancedCache().addInterceptorAfter(inter,
              cfg.getCacheMode().isDistributed() ?
                      DistLockingInterceptor.class :
                      LockingInterceptor.class);
   }

   //This is to check that both the @ProvidedId is present and the the @DocumentId is not present. This is because
   // don't want both of these 2 annotations used at the same time.

   private void validateClasses(Class... classes) {
      for (Class c : classes) {
         if (!c.isAnnotationPresent(org.hibernate.search.annotations.ProvidedId.class)) {
            throw new IllegalArgumentException("There is no provided id on " + c.getName() + " class");
         }

         for (Field field : c.getFields()) {
            if (field.getAnnotation(org.hibernate.search.annotations.DocumentId.class) != null) {
               throw new IllegalArgumentException("Please remove the documentId annotation in " + c.getName());
            }
         }

         for (Field field : c.getDeclaredFields()) {
            if (field.getAnnotation(org.hibernate.search.annotations.DocumentId.class) != null) {
               throw new IllegalArgumentException("Please remove the documentId annotation in " + c.getName());
            }
         }
      }
   }


   private void checkInterceptorChain(Cache cache) {
      // Check if there are any QueryInterceptors already added onto the chain.
      // If there already is one then throw a CacheException

      AdvancedCache advanced = cache.getAdvancedCache();

      List<CommandInterceptor> interceptorList = advanced.getInterceptorChain();

      for (CommandInterceptor inter : interceptorList) {

         if (inter.getClass().equals(QueryInterceptor.class) || inter.getClass().equals(LocalQueryInterceptor.class)) {
            throw new CacheException("There is already an instance of the QueryInterceptor running");
         }

      }
   }
}
