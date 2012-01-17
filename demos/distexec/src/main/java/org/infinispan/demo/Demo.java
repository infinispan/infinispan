/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.demo;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.SimpleJSAP;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;

import java.io.IOException;
import java.util.Iterator;

public abstract class Demo {
   protected final boolean isMaster;
   protected final String cfgFile;
   protected final JSAPResult commandLineOptions;

   public Demo(String[] args) throws Exception {
      commandLineOptions = parseParameters(args);
      String nodeType = commandLineOptions.getString("nodeType");
      isMaster = nodeType != null && nodeType.equals("master");
      cfgFile = commandLineOptions.getString("configFile");
   }

   protected JSAPResult parseParameters(String[] args) throws Exception {
      SimpleJSAP jsap = buildCommandLineOptions();

      JSAPResult config = jsap.parse(args);
      if (!config.success() || jsap.messagePrinted()) {
         Iterator<?> messageIterator = config.getErrorMessageIterator();
         while (messageIterator.hasNext()) System.err.println(messageIterator.next());
         System.err.println(jsap.getHelp());
         return null;
      }

      return config;
   }

   protected Cache<String, String> startCache() throws IOException {
      CacheBuilder cb = new CacheBuilder(cfgFile);
      EmbeddedCacheManager cacheManager = cb.getCacheManager();
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();

      cacheManager.defineConfiguration("wordcount", new ConfigurationBuilder().read(dcc)
               .clustering().l1().disable().clustering().cacheMode(CacheMode.DIST_SYNC).hash()
               .numOwners(1).build());
      Cache<String, String> cache = cacheManager.getCache();

      Transport transport = cache.getAdvancedCache().getRpcManager().getTransport();
      if (isMaster)
         System.out.printf("Node %s joined as master. View is %s.%n", transport.getAddress(), transport.getMembers());
      else
         System.out.printf("Node %s joined as slave. View is %s.%n", transport.getAddress(), transport.getMembers());

      return cache;
   }

   protected abstract SimpleJSAP buildCommandLineOptions() throws JSAPException;
}
