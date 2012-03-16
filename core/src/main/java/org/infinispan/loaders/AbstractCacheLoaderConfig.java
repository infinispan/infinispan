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
package org.infinispan.loaders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import org.infinispan.CacheException;
import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationDocRef;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.util.TypedProperties;

/**
 * Abstract base class for CacheLoaderConfigs.
 * 
 *
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @since 4.0
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class AbstractCacheLoaderConfig extends AbstractNamedCacheConfigurationBean implements CacheLoaderConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = -4303705423800914433L;

   @XmlAttribute(name="class")
   @ConfigurationDocRef(name="class", bean=AbstractCacheLoaderConfig.class,targetElement="setCacheLoaderClassName")
   protected String cacheLoaderClassName;
     
   @XmlTransient
   protected TypedProperties properties = new TypedProperties();

   public Properties getProperties() {
      return properties;
   }
   
   public void setProperties(Properties properties) {
      testImmutability("properties");
      this.properties = toTypedProperties(properties);
   }

   public void setProperties(String properties) throws IOException {
      if (properties == null) return;

      testImmutability("properties");
      // JBCACHE-531: escape all backslash characters
      // replace any "\" that is not preceded by a backslash with "\\"
      properties = XmlConfigHelper.escapeBackslashes(properties);
      ByteArrayInputStream is = new ByteArrayInputStream(properties.trim().getBytes("ISO8859_1"));
      this.properties = new TypedProperties();
      this.properties.load(is);
      is.close();
   }

   public String getCacheLoaderClassName() {
      return cacheLoaderClassName;
   }

   /** 
    * Fully qualified name of a cache loader class that must implement 
    *             org.infinispan.loaders.CacheLoader interface
    * 
    * @see org.infinispan.loaders.CacheLoaderConfig#setCacheLoaderClassName(java.lang.String)
    */
   public void setCacheLoaderClassName(String className) {
      if (className == null || className.length() == 0) return;
      testImmutability("cacheLoaderClassName");
      this.cacheLoaderClassName = className;
   }

   @Override
   public AbstractCacheLoaderConfig clone() {
      try {
         return (AbstractCacheLoaderConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new CacheException(e);
      }
   }
   
   public ClassLoader getClassLoader() {
      // TODO This is a total mess, but requires config to be re-architected to fix
      if (cr != null && cr.getComponent(Configuration.class) != null)
         return cr.getComponent(Configuration.class).getClassLoader();
      else if (Thread.currentThread().getContextClassLoader() != null)
         return Thread.currentThread().getContextClassLoader();
      else
         return null;
   }

   public void accept(ConfigurationBeanVisitor v) {}
}
