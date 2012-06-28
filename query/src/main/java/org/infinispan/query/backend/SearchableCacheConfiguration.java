/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import org.hibernate.annotations.common.reflection.ReflectionManager;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.cfg.spi.SearchConfigurationBase;
import org.hibernate.search.impl.SearchMappingBuilder;
import org.hibernate.search.infinispan.CacheManagerServiceProvider;
import org.hibernate.search.spi.ServiceProvider;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that implements {@link org.hibernate.search.cfg.SearchConfiguration} so that within Infinispan-Query, there is
 * no need for a Hibernate Core configuration object.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero
 */
public class SearchableCacheConfiguration extends SearchConfigurationBase implements SearchConfiguration {

   private final Map<String, Class<?>> classes;
   private final Properties properties;
   private final SearchMapping searchMapping;
   private final Map<Class<? extends ServiceProvider<?>>, Object> providedServices;

   public SearchableCacheConfiguration(Class<?>[] classArray, Properties properties, EmbeddedCacheManager uninitializedCacheManager, ComponentRegistry cr) {
      this.providedServices = initializeProvidedServices(uninitializedCacheManager, cr);
      if (properties == null) {
         this.properties = new Properties();
      }
      else {
         this.properties = properties;
      }

      classes = new HashMap<String, Class<?>>();

      for (Class<?> c : classArray) {
         String classname = c.getName();
         classes.put(classname, c);
      }

      //deal with programmatic mapping:
      searchMapping = SearchMappingBuilder.getSearchMapping(this);

      //if we have a SearchMapping then we can predict at least those entities specified in the mapping
      //and avoid further SearchFactory rebuilds triggered by new entity discovery during cache events
      if ( searchMapping != null ) {
         Set<Class<?>> mappedEntities = searchMapping.getMappedEntities();
         for (Class<?> entity : mappedEntities) {
            classes.put(entity.getName(), entity);
         }
      }
   }

   private static Map<Class<? extends ServiceProvider<?>>, Object> initializeProvidedServices(EmbeddedCacheManager uninitializedCacheManager, ComponentRegistry cr) {
      //Register the SelfLoopedCacheManagerServiceProvider to allow custom IndexManagers to access the CacheManager
      ConcurrentHashMap map = new ConcurrentHashMap(2);
      map.put(CacheManagerServiceProvider.class, uninitializedCacheManager);
      map.put(ComponentRegistryServiceProvider.class, cr);
      return Collections.unmodifiableMap(map);
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
   public Map<Class<? extends ServiceProvider<?>>, Object> getProvidedServices() {
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

}
