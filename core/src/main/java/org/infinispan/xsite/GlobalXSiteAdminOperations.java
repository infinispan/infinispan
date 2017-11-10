package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.xsite.status.ContainerSiteStatusBuilder;
import org.infinispan.xsite.status.SiteStatus;

/**
 * A per-container (cache manager) cross-site admin operations.
 * <p>
 * All the operations invoked in this class will be applied to all caches which remotely backups its data.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = "GlobalXSiteAdminOperations", description = "Exposes tooling for handling backing up data to remote sites.")
public class GlobalXSiteAdminOperations {

   public static final String CACHE_DELIMITER = ",";

   @Inject private EmbeddedCacheManager cacheManager;

   private static void addCacheAdmin(Cache cache, List<CacheXSiteAdminOperation> list) {
      if (cache != null) {
         ComponentRegistry cacheRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
         XSiteAdminOperations operation = cacheRegistry.getComponent(XSiteAdminOperations.class);
         if (operation != null) {
            list.add(new CacheXSiteAdminOperation(cache.getName(), operation));
         }
      }
   }

   @ManagedOperation(
         description = "Takes this site offline in all caches in the cluster.",
         displayName = "Takes this site offline in all caches in the cluster."
   )
   public String takeSiteOffline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      return performOperation(operations -> operations.takeSiteOffline(site));
   }

   @ManagedOperation(
         description = "Brings the given site back online on all the caches.",
         displayName = "Brings the given site back online on all the caches."
   )
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      return performOperation(operations -> operations.bringSiteOnline(site));
   }

   @ManagedOperation(
         displayName = "Push state to site",
         description = "Pushes the state of all caches to the corresponding remote site if the cache backups to it. " +
               "The remote site will be bring back online",
         name = "pushState"
   )
   public final String pushState(@Parameter(description = "The destination site name", name = "SiteName") String site) {
      return performOperation(operations -> operations.pushState(site));
   }

   @ManagedOperation(
         displayName = "Cancel Push State",
         description = "Cancels the push state on all the caches to remote site.",
         name = "CancelPushState"
   )
   public final String cancelPushState(@Parameter(description = "The destination site name", name = "SiteName")
                                       final String site) {
      return performOperation(operations -> operations.cancelPushState(site));
   }

   public final Map<String, SiteStatus> globalStatus() {
      final Iterator<CacheXSiteAdminOperation> iterator = collectXSiteAdminOperation().iterator();
      if (!iterator.hasNext()) {
         return Collections.emptyMap();
      }
      Map<String, ContainerSiteStatusBuilder> siteStatusBuilderMap = new HashMap<>();
      while (iterator.hasNext()) {
         CacheXSiteAdminOperation xsiteAdminOperation = iterator.next();
         xsiteAdminOperation.xSiteAdminOperations.clusterStatus().forEach((site, status) -> {
            ContainerSiteStatusBuilder builder = siteStatusBuilderMap.get(site);
            if (builder == null) {
               builder = new ContainerSiteStatusBuilder();
               siteStatusBuilderMap.put(site, builder);
            }
            builder.addCacheName(xsiteAdminOperation.cacheName, status);
         });
      }

      Map<String, SiteStatus> result = new HashMap<>();
      siteStatusBuilderMap.forEach((site, builder) -> result.put(site, builder.build()));
      return result;
   }

   private String performOperation(Operation operation) {
      final Iterator<CacheXSiteAdminOperation> iterator = collectXSiteAdminOperation().iterator();
      if (!iterator.hasNext()) {
         return XSiteAdminOperations.SUCCESS;
      }
      StringBuilder builder = new StringBuilder();
      while (iterator.hasNext()) {
         CacheXSiteAdminOperation xsiteAdminOperation = iterator.next();
         try {
            String result = operation.execute(xsiteAdminOperation.xSiteAdminOperations);
            builder.append(xsiteAdminOperation.cacheName).append(": ").append(result);
            if (iterator.hasNext()) {
               builder.append(CACHE_DELIMITER);
            }
         } catch (Exception e) {
            builder.append("Exception on ").append(xsiteAdminOperation.cacheName).append(": ").append(e.getMessage());
         }
      }
      return builder.length() == 0 ? XSiteAdminOperations.SUCCESS : builder.toString();
   }

   private Collection<CacheXSiteAdminOperation> collectXSiteAdminOperation() {
      Collection<String> cacheNames = cacheManager.getCacheNames();
      List<CacheXSiteAdminOperation> operations = new ArrayList<>(cacheNames.size() + 1);
      for (String cacheName : cacheNames) {
         addCacheAdmin(cacheManager.getCache(cacheName, false), operations);
      }
      if (cacheManager.getDefaultCacheConfiguration() != null) {
         addCacheAdmin(cacheManager.getCache(), operations);
      }
      return operations;
   }

   private interface Operation {
      String execute(XSiteAdminOperations admins);
   }

   private static class CacheXSiteAdminOperation {
      private final String cacheName;
      private final XSiteAdminOperations xSiteAdminOperations;

      private CacheXSiteAdminOperation(String cacheName, XSiteAdminOperations xSiteAdminOperations) {
         this.cacheName = cacheName;
         this.xSiteAdminOperations = xSiteAdminOperations;
      }
   }

}
