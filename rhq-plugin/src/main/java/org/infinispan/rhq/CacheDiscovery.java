/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.rhq;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jmx.MBeanResourceDiscoveryComponent;
import org.rhq.plugins.jmx.ObjectNameQueryUtility;

import javax.management.ObjectName;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Discovery class for individual cache instances
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheDiscovery extends MBeanResourceDiscoveryComponent<CacheManagerComponent> {
   private static final Log log = LogFactory.getLog(CacheDiscovery.class);

   /** Run the discovery */
   public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<CacheManagerComponent> ctx) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Discover resources with context");
      Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();

      EmsConnection conn = ctx.getParentResourceComponent().getEmsConnection();
      if (trace) log.trace("Connection to ems server established");

      String pattern = getAllCachesPattern(ctx.getParentResourceContext().getResourceKey());
      if (trace) log.tracef("Pattern to query is %s", pattern);

      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      if (trace) log.tracef("Querying [%s] returned beans: %s", queryUtility.getTranslatedQuery(), beans);

      for (EmsBean bean : beans) {
         /* A discovered resource must have a unique key, that must
          * stay the same when the resource is discovered the next
          * time */
         String name = bean.getAttribute("CacheName").getValue().toString();
         String mbeanCacheName = bean.getBeanName().getKeyProperty("name");
         if (trace) log.tracef("Resource name is %s and resource key %s", name, mbeanCacheName);
         DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
               ctx.getResourceType(), // Resource Type
               mbeanCacheName, // Resource Key
               name, // Resource name 
               null, // Version
               "One cache within Infinispan", // ResourceDescription
               ctx.getDefaultPluginConfiguration(), // Plugin Config
               null // ProcessInfo
         );

         // Add to return values
         discoveredResources.add(detail);
         log.info("Discovered new ...  " + bean.getBeanName().getCanonicalName());
      }
      return discoveredResources;
   }

   private String getAllCachesPattern(String cacheManagerName) {
      return cacheComponentPattern(cacheManagerName, "Cache") + ",*";
   }

   protected static String cacheComponentPattern(String cacheManagerName, String componentName) {
      return "*:type=Cache,component=" + componentName + ",manager=" + ObjectName.quote(cacheManagerName);
   }
}
