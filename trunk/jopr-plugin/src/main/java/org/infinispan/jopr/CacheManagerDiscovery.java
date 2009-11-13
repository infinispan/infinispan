/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.jopr;

import static org.infinispan.jmx.CacheManagerJmxRegistration.*;
import static org.infinispan.jmx.ComponentsJmxRegistration.*;
import static org.infinispan.manager.DefaultCacheManager.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jmx.JMXDiscoveryComponent;
import org.rhq.plugins.jmx.ObjectNameQueryUtility;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Discovery class for Infinispan engines
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheManagerDiscovery implements ResourceDiscoveryComponent<CacheManagerComponent> {
   private static final Log log = LogFactory.getLog(CacheManagerDiscovery.class);

   // Assume a java5+ jmx-remote connector on port 6996
   public static String REMOTE = "service:jmx:rmi://127.0.0.1/jndi/rmi://127.0.0.1:6996/jmxrmi";

   private static final String MANAGER_OBJECT = "*:" + CACHE_NAME_KEY + '=' + GLOBAL_JMX_GROUP + "," + JMX_RESOURCE_KEY + "=" + OBJECT_NAME;
   private static final String CONNECTOR = "org.mc4j.ems.connection.support.metadata.J2SE5ConnectionTypeDescriptor";
   private static final String OBJECT_NAME_KEY = "objectName";
   
   /**
    * Run the discovery
    */
   public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<CacheManagerComponent> ctx) throws Exception {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Discover resources with context: {0}", ctx);
      Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();
      DiscoveredResourceDetails resource = null;
      List<Configuration> manualCfgs = ctx.getPluginConfigurations();
      if (!manualCfgs.isEmpty()) {
         // Process any manually-added resource.
         Configuration cfg = ctx.getPluginConfigurations().get(0);
         String objectName = cfg.getSimple(OBJECT_NAME_KEY).getStringValue();
         String connectorAddress = cfg.getSimple(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY).getStringValue();
         resource = createDiscoveredResource(ctx, objectName, connectorAddress);
         if (trace) log.trace("Manually discovered resource is {0}", resource);
      } else {
         // Process auto discovered resource
         resource = createDiscoveredResource(ctx, MANAGER_OBJECT, REMOTE);
         if (trace) log.trace("Automatically discovered resource is {0}", resource);
         discoveredResources.add(resource);
      }
      discoveredResources.add(resource);
      return discoveredResources;
   }

   private DiscoveredResourceDetails createDiscoveredResource(ResourceDiscoveryContext ctx, String objectName, String connectorAddress) {
      boolean trace = log.isTraceEnabled();

      Configuration c = new Configuration();
      c.put(new PropertySimple(OBJECT_NAME_KEY, objectName));
      c.put(new PropertySimple(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY, connectorAddress));
      c.put(new PropertySimple(JMXDiscoveryComponent.CONNECTION_TYPE, CONNECTOR));
      if (trace) log.trace("Configuration is {0}", c.toString(true));
      
      ConnectionHelper helper = new ConnectionHelper();
      EmsConnection conn = helper.getEmsConnection(c);
      if (conn != null) {
         if (trace) log.trace("Connection to ems server stablished: {0}", conn);

         // Run query for manager_object
         ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(objectName);
         List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
         if (trace) log.trace("Querying [{0}] returned beans: {1}", queryUtility.getTranslatedQuery(), beans);

         EmsBean bean = beans.get(0);
         String managerName = bean.getBeanName().getCanonicalName();
         String resourceName = bean.getAttribute("Name").getValue().toString();
         String version = bean.getAttribute("Version").getValue().toString();
         /* A discovered resource must have a unique key, that must stay the same when the resource is discovered the next time */
         if (trace) log.trace("Add resource with version '{1}' and type {2}", version, ctx.getResourceType());
         DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
               ctx.getResourceType(), // Resource type
               resourceName, // Resource key
               resourceName, // Resource name
               version, // Resource version
               "A cache manager within Infinispan", // Description
               c, // Plugin config
               null // Process info from a process scan
         );
         log.info("Discovered Infinispan instance with key {0} and name {1}", resourceName, managerName);
         return detail;
      } else {
         log.debug("Unable to establish connection");
         return null;
      }
   }
}