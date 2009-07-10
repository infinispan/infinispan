/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config.parsing;

import java.lang.reflect.Method;
import java.util.Set;

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.util.Util;
import org.w3c.dom.Element;

public class CacheLoaderManagerConfigReader implements ConfigurationElementReader {

   private AutomatedXmlConfigurationParserImpl parser;

   public CacheLoaderManagerConfigReader() {
      super();
   }

   public void setParser(AutomatedXmlConfigurationParserImpl parser) {
      this.parser = parser;
   }

   public void process(Element e, AbstractConfigurationBean bean) {      
      CacheLoaderManagerConfig cBean = (CacheLoaderManagerConfig) parser.findAndInstantiateBean(e);
      
      //set attributes of <loaders/>
      for (Method m : cBean.getClass().getMethods()) {
         boolean setter = m.getName().startsWith("set") && m.getParameterTypes().length == 1;
         if (setter) {
            parser.reflectAndInvokeAttribute(cBean, m, e);
            parser.reflectAndInvokeProperties(cBean, m, e);              
         }
      }
      
      Set<Element> elements = parser.getAllElementsInCoreNS("loader", e);
      if (elements.isEmpty())
         throw new ConfigurationException("No loader elements found!");

      for (Element element : elements) {
         String clClass = parser.getAttributeValue(element, "class");
         if (!parser.existsAttribute(clClass))
            throw new ConfigurationException("Missing 'class'  attribute for cache loader configuration");

         CacheLoader cl;
         CacheLoaderConfig clc;
         try {
            cl = (CacheLoader) Util.getInstance(clClass);
            clc = Util.getInstance(cl.getConfigurationClass());
         } catch (Exception ex) {
            throw new ConfigurationException("Unable to instantiate cache loader or configuration",ex);
         }

         clc.setCacheLoaderClassName(clClass);
         Element propertiesElement = parser.getSingleElementInCoreNS("properties", element);
         if (propertiesElement == null)
            throw new ConfigurationException("loader " + clClass + " is missing properties element");
         parser.visitElement(element, (AbstractConfigurationBean) clc);
         cBean.addCacheLoaderConfig(clc);
      }
      ((Configuration)bean).setCacheLoaderManagerConfig(cBean);
   }
}
