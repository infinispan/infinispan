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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.Util;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class CustomInterceptorConfigReader implements ConfigurationElementReader {

   private AutomatedXmlConfigurationParserImpl parser;

   public CustomInterceptorConfigReader() {
      super();
   }

   public void setParser(AutomatedXmlConfigurationParserImpl parser) {
      this.parser = parser;
   }

   public void process(Element e, AbstractConfigurationBean bean) {
      NodeList interceptorNodes = e.getElementsByTagName("interceptor");
      List<CustomInterceptorConfig> interceptorConfigs = new ArrayList<CustomInterceptorConfig>(interceptorNodes.getLength());
      for (int i = 0; i < interceptorNodes.getLength(); i++) {
         boolean first = false;
         boolean last = false;
         int index = -1;
         String after = null;
         String before = null;

         Element interceptorElement = (Element) interceptorNodes.item(i);
         String position = parser.getAttributeValue(interceptorElement, "position");
         if (parser.existsAttribute(position) && "first".equalsIgnoreCase(position)) {
            first = true;
         }
         if (parser.existsAttribute(position) && "last".equalsIgnoreCase(position)) {
            last = true;
         }
         String indexStr = parser.getAttributeValue(interceptorElement, "index");
         index = parser.existsAttribute(indexStr) ? parser.getInt(indexStr) : -1;

         before = parser.getAttributeValue(interceptorElement, "before");
         if (!parser.existsAttribute(before))
            before = null;
         after = parser.getAttributeValue(interceptorElement, "after");
         if (!parser.existsAttribute(after))
            after = null;

         CommandInterceptor interceptor = buildCommandInterceptor(interceptorElement);
         CustomInterceptorConfig customInterceptorConfig = new CustomInterceptorConfig(interceptor,
                  first, last, index, after, before);
         interceptorConfigs.add(customInterceptorConfig);
      }
      ((Configuration) bean).setCustomInterceptors(interceptorConfigs);
   }

   /**
    * Builds the interceptor based on the interceptor class and also sets all its attributes.
    */
   private CommandInterceptor buildCommandInterceptor(Element element) {
      String interceptorClass = parser.getAttributeValue(element, "class");
      if (!parser.existsAttribute(interceptorClass))
         throw new ConfigurationException("Interceptor class cannot be empty!");
      CommandInterceptor result;
      try {
         result = (CommandInterceptor) Util.loadClass(interceptorClass).newInstance();
      } catch (Exception e) {
         throw new ConfigurationException(
                  "CommandInterceptor class is not properly loaded in classloader", e);
      }
      Properties p = XmlConfigHelper.extractProperties(element);
      if (p != null)
         XmlConfigHelper.setValues(result, p, false, true);
      return result;
   }
}
