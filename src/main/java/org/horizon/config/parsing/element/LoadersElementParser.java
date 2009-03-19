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

import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.ConfigurationException;
import org.horizon.config.parsing.XmlConfigHelper;
import org.horizon.config.parsing.XmlParserBase;
import org.horizon.loader.CacheLoader;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.decorators.AsyncStoreConfig;
import org.horizon.loader.decorators.SingletonStoreConfig;
import org.horizon.util.Util;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Properties;

/**
 * Utility class for parsing the 'loaders' element in the .xml configuration file.
 * <pre>
 * Note: class does not rely on element position in the configuration file.
 *       It does not rely on element's name either.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class LoadersElementParser extends XmlParserBase {
   public CacheLoaderManagerConfig parseLoadersElement(Element element) {
      CacheLoaderManagerConfig cacheLoaderConfig = new CacheLoaderManagerConfig();
      String passivation = getAttributeValue(element, "passivation");
      if (existsAttribute(passivation)) cacheLoaderConfig.setPassivation(getBoolean(passivation));
      String shared = getAttributeValue(element, "shared");
      if (existsAttribute(shared)) cacheLoaderConfig.setShared(getBoolean(shared));
      boolean preload = getBoolean(getAttributeValue(element, "preload"));
      cacheLoaderConfig.setPreload(preload);

      NodeList cacheLoaderNodes = element.getElementsByTagName("loader");
      for (int i = 0; i < cacheLoaderNodes.getLength(); i++) {
         Element indivElement = (Element) cacheLoaderNodes.item(i);
         CacheLoaderConfig iclc = parseIndividualCacheLoaderConfig(indivElement);
         cacheLoaderConfig.addCacheLoaderConfig(iclc);
      }
      return cacheLoaderConfig;
   }

   private CacheLoaderConfig parseIndividualCacheLoaderConfig(Element indivElement) {
      String clClass = getAttributeValue(indivElement, "class");
      if (!existsAttribute(clClass))
         throw new ConfigurationException("Missing 'class'  attribute for cache loader configuration");
      CacheLoader cl;
      CacheLoaderConfig clc;
      try {
         cl = (CacheLoader) Util.getInstance(clClass);
         clc = Util.getInstance(cl.getConfigurationClass());
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate cache loader or configuration", e);
      }

      String fetchPersistentState = getAttributeValue(indivElement, "fetchPersistentState");
      if (existsAttribute(fetchPersistentState)) clc.setFetchPersistentState(getBoolean(fetchPersistentState));
      String ignoreModifications = getAttributeValue(indivElement, "ignoreModifications");
      if (existsAttribute(ignoreModifications)) clc.setIgnoreModifications(getBoolean(ignoreModifications));
      String purgeOnStartup = getAttributeValue(indivElement, "purgeOnStartup");
      if (existsAttribute(purgeOnStartup)) clc.setPurgeOnStartup(getBoolean(purgeOnStartup));

      clc.setCacheLoaderClassName(clClass);
      Element propertiesElement = getSingleElementInCoreNS("properties", indivElement);
      Properties props = XmlConfigHelper.extractProperties(propertiesElement);
      if (props != null) XmlConfigHelper.setValues(clc, props, false, true);

      clc.setSingletonStoreConfig(parseSingletonStoreConfig(getSingleElementInCoreNS("singletonStore", indivElement)));
      clc.setAsyncStoreConfig(parseAsyncStoreConfig(getSingleElementInCoreNS("async", indivElement)));
      return clc;
   }

   public SingletonStoreConfig parseSingletonStoreConfig(Element element) {
      SingletonStoreConfig ssc = new SingletonStoreConfig();
      if (element == null) {
         ssc.setSingletonStoreEnabled(false);
      } else {
         boolean singletonStoreEnabled = getBoolean(getAttributeValue(element, "enabled"));
         ssc.setSingletonStoreEnabled(singletonStoreEnabled);

         String tmp = getAttributeValue(element, "pushStateWhenCoordinator");
         if (existsAttribute(tmp)) ssc.setPushStateWhenCoordinator(getBoolean(tmp));

         tmp = getAttributeValue(element, "pushStateTimeout");
         if (existsAttribute(tmp)) ssc.setPushStateTimeout(getLong(tmp));
      }
      return ssc;
   }

   public AsyncStoreConfig parseAsyncStoreConfig(Element element) {
      AsyncStoreConfig asc = new AsyncStoreConfig();
      if (element == null) {
         asc.setEnabled(false);
      } else {
         boolean async = getBoolean(getAttributeValue(element, "enabled"));
         asc.setEnabled(async);

         if (async) {
            String tmp = getAttributeValue(element, "batchSize");
            if (existsAttribute(tmp)) asc.setBatchSize(getInt(tmp));

            tmp = getAttributeValue(element, "pollWait");
            if (existsAttribute(tmp)) asc.setPollWait(getLong(tmp));

            tmp = getAttributeValue(element, "queueSize");
            if (existsAttribute(tmp)) asc.setQueueSize(getInt(tmp));

            tmp = getAttributeValue(element, "threadPoolSize");
            if (existsAttribute(tmp)) asc.setThreadPoolSize(getInt(tmp));
         }
      }
      return asc;
   }
}
