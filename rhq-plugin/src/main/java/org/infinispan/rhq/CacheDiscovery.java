package org.infinispan.rhq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jmx.MBeanResourceDiscoveryComponent;
import org.rhq.plugins.jmx.util.ObjectNameQueryUtility;

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
   @Override
   public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<CacheManagerComponent> ctx) {
      boolean trace = log.isTraceEnabled();
      if (trace) log.trace("Discover resources with context");
      Set<DiscoveredResourceDetails> discoveredResources = new HashSet<>();

      EmsConnection conn = ctx.getParentResourceComponent().getEmsConnection();
      if (trace) log.trace("Connection to ems server established");

      String pattern = getAllCachesPattern(ctx.getParentResourceContext().getResourceKey());
      if (trace) log.trace("Pattern to query is " + pattern);

      ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(pattern);
      List<EmsBean> beans = conn.queryBeans(queryUtility.getTranslatedQuery());
      if (trace) log.trace("Querying "+queryUtility.getTranslatedQuery()+" returned beans: " + beans);

      for (EmsBean bean : beans) {
         // Filter out spurious beans
         if (CacheComponent.isCacheComponent(bean, "Cache")) {
            /* A discovered resource must have a unique key, that must
             * stay the same when the resource is discovered the next
             * time */
            String name = bean.getAttribute("CacheName").getValue().toString();
            String mbeanCacheName = bean.getBeanName().getKeyProperty("name");
            if (trace) log.trace("Resource name is "+name+" and resource key "+ mbeanCacheName);
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
         } else {
            log.warn(String.format("MBeanServer returned spurious object %s", bean.getBeanName().getCanonicalName()));
         }
      }
      return discoveredResources;
   }

   protected static String getAllCachesPattern(String cacheManagerName) {
      return cacheComponentPattern(cacheManagerName, "*", "Cache");
   }

   protected static String cacheComponentPattern(String cacheManagerName, String cacheName, String componentName) {
      // The CacheManager registers like <domain>:name=<name> so add those to request
      String cacheAttributeName;
      String type;

      // the query components do not follow the same naming conventions as the core components and need special handling
      if (componentName.equals("MassIndexer")) {
         cacheAttributeName = "cache";
         // drop the "(cacheMode)" suffix from cache name
         if (cacheName.endsWith(")\"")) {
            cacheName = cacheName.substring(0, cacheName.length() - 2);
            cacheName = cacheName.substring(0, cacheName.lastIndexOf('(')) + "\"";
         }
         type = "Query";
      } else {
         cacheAttributeName = "name";
         type = "Cache";
      }

      return cacheManagerName.substring(0, cacheManagerName.indexOf(":")) + ":manager=" +
            cacheManagerName.substring(cacheManagerName.indexOf("=") + 1)
            + ",type=" + type
            + ",component=" + componentName
            + "," + cacheAttributeName + "=" + cacheName;
   }
}
