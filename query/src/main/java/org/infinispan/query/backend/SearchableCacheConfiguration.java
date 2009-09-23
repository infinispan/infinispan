/*
 * JBoss, Home of Professional Open Source
 * Copyright ${year}, Red Hat Middleware LLC, and individual contributors
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
import org.hibernate.annotations.common.reflection.ReflectionManager;

import java.util.Iterator;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * Class that implements {@link org.hibernate.search.cfg.SearchConfiguration} so that within Infinispan-Query, there is
 * no need for a Hibernate Core configuration object.
 *
 * @author Navin Surtani
 */
public class SearchableCacheConfiguration implements SearchConfiguration {
   protected Map<String, Class> classes;
   private Properties properties;

   public SearchableCacheConfiguration(Class[] classArray, Properties properties) {
      // null chks
      if (classArray == null) throw new NullPointerException("Classes provided are null");
      this.properties = properties;
      if (this.properties == null) this.properties = new Properties();

      classes = new HashMap<String, Class>();

      // loop thru your classArray
      // populate your Map

      for (Class c : classArray) {
         String classname = c.getName();
         classes.put(classname, c);
      }
   }

   public Iterator getClassMappings() {
      return classes.values().iterator();
   }

   public Class getClassMapping(String name) {
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


   //TODO: Will have to be uncommented when dependency is changed to HS 3.2

//   public SearchMapping getProgrammaticMapping() {
//
//      // Documentation on interface says "returns the programmatic configuration or null".
//      // Since I don't have this parameter set on my implementation, I will just return this
//      // as a null.
//
//      return null;
//   }


}
