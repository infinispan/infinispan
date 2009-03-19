/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.horizon.config.parsing.element;


import org.horizon.config.ConfigurationException;
import org.horizon.config.CustomInterceptorConfig;
import org.horizon.config.parsing.XmlConfigHelper;
import org.horizon.config.parsing.XmlParserBase;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.util.Util;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Utility class for parsing 'buddy' element in the .xml configuration file.
 * <pre>
 * Note: class does not rely on element position in the configuration file.
 *       It does not rely on element's name either.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CustomInterceptorsElementParser extends XmlParserBase {
   /**
    * Iterates within the given element looking for custom interceptors.
    *
    * @param element should not be null
    * @return a list which might be empty, never null
    */
   public List<CustomInterceptorConfig> parseCustomInterceptors(Element element) {
      NodeList interceptorNodes = element.getElementsByTagName("interceptor");
      List<CustomInterceptorConfig> interceptorConfigs = new ArrayList<CustomInterceptorConfig>(interceptorNodes.getLength());
      for (int i = 0; i < interceptorNodes.getLength(); i++) {
         boolean first = false;
         boolean last = false;
         int index = -1;
         String after = null;
         String before = null;

         Element interceptorElement = (Element) interceptorNodes.item(i);
         String position = getAttributeValue(interceptorElement, "position");
         if (existsAttribute(position) && "first".equalsIgnoreCase(position)) {
            first = true;
         }
         if (existsAttribute(position) && "last".equalsIgnoreCase(position)) {
            last = true;
         }
         String indexStr = getAttributeValue(interceptorElement, "index");
         index = existsAttribute(indexStr) ? getInt(indexStr) : -1;

         before = getAttributeValue(interceptorElement, "before");
         if (!existsAttribute(before)) before = null;
         after = getAttributeValue(interceptorElement, "after");
         if (!existsAttribute(after)) after = null;

         CommandInterceptor interceptor = buildCommandInterceptor(interceptorElement);

         CustomInterceptorConfig customInterceptorConfig = new CustomInterceptorConfig(interceptor, first, last, index, after, before);
         interceptorConfigs.add(customInterceptorConfig);
      }
      return interceptorConfigs;
   }

   /**
    * Builds the interceptor based on the interceptor class and also sets all its attributes.
    */
   private CommandInterceptor buildCommandInterceptor(Element element) {
      String interceptorClass = getAttributeValue(element, "class");
      if (!existsAttribute(interceptorClass)) throw new ConfigurationException("Interceptor class cannot be empty!");
      CommandInterceptor result;
      try {
         result = (CommandInterceptor) Util.loadClass(interceptorClass).newInstance();
      }
      catch (Exception e) {
         throw new ConfigurationException("CommandInterceptor class is not properly loaded in classloader", e);
      }
      Properties p = XmlConfigHelper.extractProperties(element);
      XmlConfigHelper.setValues(result, p, false, true);
      return result;

   }
}
