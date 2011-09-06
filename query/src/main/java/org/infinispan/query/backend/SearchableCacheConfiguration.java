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
import org.hibernate.search.spi.ServiceProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Class that implements {@link org.hibernate.search.cfg.SearchConfiguration} so that within Infinispan-Query, there is
 * no need for a Hibernate Core configuration object.
 *
 * @author Navin Surtani
 */
public class SearchableCacheConfiguration implements SearchConfiguration {
   protected Map<String, Class<?>> classes;
   private Properties properties;

   public SearchableCacheConfiguration(Class[] classArray, Properties properties) {
      // null chks
      if (classArray == null) throw new NullPointerException("Classes provided are null");
      this.properties = properties;
      if (this.properties == null) this.properties = new Properties();

      classes = new HashMap<String, Class<?>>();

      // loop thru your classArray
      // populate your Map

      for (Class c : classArray) {
         String classname = c.getName();
         classes.put(classname, c);
      }
   }

   public Iterator<Class<?>> getClassMappings() {
      return classes.values().iterator();
   }

   public Class<?> getClassMapping(String name) {
      return classes.get(name);
   }

   public String getProperty(String propertyName) {
      return properties.getProperty(propertyName);
   }

   public Properties getProperties() {
      return properties;
   }

   public ReflectionManager getReflectionManager() {
      return null;
   }

   @Override
   public SearchMapping getProgrammaticMapping() {
      // TODO What does Hibernate Search expect here?
      return null;
   }

   @Override
   public Map<Class<? extends ServiceProvider<?>>, Object> getProvidedServices() {
      return Collections.emptyMap();
   }

   @Override
   public boolean isTransactionManagerExpected() {
      return false;
   }
}
