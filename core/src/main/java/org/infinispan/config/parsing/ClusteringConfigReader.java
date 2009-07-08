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

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ClusteringConfigReader implements ConfigurationElementReader {

   private AutomatedXmlConfigurationParserImpl parser;

   public ClusteringConfigReader() {
      super();
   }

   public void setParser(AutomatedXmlConfigurationParserImpl parser) {
      this.parser = parser;
   }

   public void process(Element e, AbstractConfigurationBean bean) {
      if (e == null) return; //we might not have this configured

      Configuration config = (Configuration) bean;
      Configuration.CacheMode cacheMode;
      String mode = parser.getAttributeValue(e, "mode").toUpperCase();
      if (mode.startsWith("R"))
         cacheMode = Configuration.CacheMode.REPL_SYNC;
      else if (mode.startsWith("I"))
         cacheMode = Configuration.CacheMode.INVALIDATION_SYNC;
      else
         cacheMode = Configuration.CacheMode.DIST_SYNC; // the default

      Element asyncEl = parser.getSingleElementInCoreNS("async", e);
      Element syncEl = parser.getSingleElementInCoreNS("sync", e);
      if (syncEl != null && asyncEl != null)
         throw new ConfigurationException("Cannot have sync and async elements within the same cluster element!");
      boolean sync = asyncEl == null; // even if both are null, we default to sync
      if (sync) {
         config.setCacheMode(cacheMode);        
      } else {
         cacheMode = cacheMode.toAsync(); // get the async version of this mode
         config.setCacheMode(cacheMode);
      }
      
      NodeList nodeList = e.getChildNodes();
      for (int numChildren = nodeList.getLength(), i = 0; i < numChildren; i++) {
         Node node = nodeList.item(i);
         if (node instanceof Element) {
            Element childNode = (Element) node;
            // recursive step
            parser.visitElement(childNode, bean);
         }
      }    
   }
}
