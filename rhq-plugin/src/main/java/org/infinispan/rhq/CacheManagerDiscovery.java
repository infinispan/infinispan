package org.infinispan.rhq;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ManualAddFacet;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.MBeanResourceDiscoveryComponent;
import org.rhq.plugins.jmx.util.ObjectNameQueryUtility;

/**
 * Discovery class for Infinispan engines
 *
 * @author Heiko W. Rupp
 * @author Galder Zamarreño
 */
public class CacheManagerDiscovery extends MBeanResourceDiscoveryComponent<JMXComponent<?>> implements ManualAddFacet<JMXComponent<?>>{
   private static final Log log = LogFactory.getLog(CacheManagerDiscovery.class);
   public static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager,component=CacheManager";

   protected static final String CACHE_MANAGER_OBJECTS = "*:" + CACHE_MANAGER_JMX_GROUP + ",*";

   /**
    * Run the discovery
    */
   @Override
   public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<JMXComponent<?>> ctx) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Discover resources with context " + ctx);
      // Process auto discovered resource
      Set<DiscoveredResourceDetails> discoveredResources = createDiscoveredResource(ctx, null, CACHE_MANAGER_OBJECTS);
      if (trace) log.trace("Automatically discovered resources are " + discoveredResources);

      return discoveredResources;
   }

   private Set<DiscoveredResourceDetails> createDiscoveredResource(ResourceDiscoveryContext<JMXComponent<?>> ctx, Configuration pluginConfiguration, String objectName) {
      boolean trace = log.isTraceEnabled();
      JMXComponent<?> parentComponent = ctx.getParentResourceComponent();
      EmsConnection conn = parentComponent.getEmsConnection();
      if (conn != null) {
         if (trace) log.trace("Connection to ems server established: " + conn);

         // Run query for manager_object
         ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(objectName);
         List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
         if (trace) log.trace("Querying ["+queryUtility.getTranslatedQuery()+"] returned beans: " + beans);

         Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();
         for (EmsBean bean : beans) {
            // Filter out spurious beans
            if (CacheManagerComponent.isCacheManagerComponent(bean)) {
               String managerName = bean.getBeanName().getCanonicalName();
               String resourceName = bean.getAttribute("Name").getValue().toString();
               String version = bean.getAttribute("Version").getValue().toString();
               /* A discovered resource must have a unique key, that must stay the same when the resource is discovered the next time */
               if (trace) log.trace("Add resource with version '"+version+"' and type " + ctx.getResourceType());
               DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
                     ctx.getResourceType(), // Resource type
                     resourceName, // Resource key
                     resourceName, // Resource name
                     version, // Resource version
                     "A cache manager within Infinispan", // Description
                     pluginConfiguration, // Plugin config
                     null // Process info from a process scan
               );
               if(log.isInfoEnabled()) {
                  log.info(String.format("Discovered Infinispan instance with key %s and name %s", resourceName, managerName));
               }
               discoveredResources.add(detail);
            } else {
               log.warn(String.format("MBeanServer returned spurious object %s", bean.getBeanName().getCanonicalName()));
            }
         }
         return discoveredResources;
      } else {
         log.debug("Unable to establish connection");
         return null;
      }
   }

   @Override
   public DiscoveredResourceDetails discoverResource(Configuration pluginConfiguration,
         ResourceDiscoveryContext<JMXComponent<?>> ctx) throws InvalidPluginConfigurationException {
      Set<DiscoveredResourceDetails> discoveredResources = createDiscoveredResource(ctx, pluginConfiguration, CACHE_MANAGER_OBJECTS);
      if (log.isTraceEnabled()) log.trace("Manually discovered resource: " + discoveredResources);
      if(discoveredResources.size()>0) {
         return discoveredResources.iterator().next();
      } else {
         throw new InvalidPluginConfigurationException("Expecting single resource, found "+discoveredResources.size());
      }
   }
}
