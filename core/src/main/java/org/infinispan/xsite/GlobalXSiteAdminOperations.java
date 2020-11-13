package org.infinispan.xsite;

import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
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

   @Inject EmbeddedCacheManager cacheManager;

   private static void addCacheAdmin(Cache cache, List<CacheXSiteAdminOperation> list) {
      if (cache != null) {
         ComponentRegistry cacheRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
         XSiteAdminOperations operation = cacheRegistry.getComponent(XSiteAdminOperations.class);
         if (operation != null) {
            list.add(new CacheXSiteAdminOperation(cache.getName(), operation));
         }
      }
   }

   public Map<String, String> takeAllCachesOffline(String site) {
      return performMultiCacheOperation(operations -> operations.takeSiteOffline(site));
   }

   public Map<String, String> bringAllCachesOnline(String site) {
      return performMultiCacheOperation(operations -> operations.bringSiteOnline(site));
   }

   public Map<String, String> pushStateAllCaches(String site) {
      return performMultiCacheOperation(operations -> operations.pushState(site));
   }

   public Map<String, String> cancelPushStateAllCaches(String site) {
      return performMultiCacheOperation(operations -> operations.cancelPushState(site));
   }

   private String toJMXResponse(Map<String, String> results) {
      return results.entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining(CACHE_DELIMITER));
   }

   @ManagedOperation(
         description = "Takes this site offline in all caches in the cluster.",
         displayName = "Takes this site offline in all caches in the cluster."
   )
   public String takeSiteOffline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      return toJMXResponse(performMultiCacheOperation(operations -> operations.takeSiteOffline(site)));
   }

   @ManagedOperation(
         description = "Brings the given site back online on all the caches.",
         displayName = "Brings the given site back online on all the caches."
   )
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      return toJMXResponse(performMultiCacheOperation(operations -> operations.bringSiteOnline(site)));
   }

   @ManagedOperation(
         displayName = "Push state to site",
         description = "Pushes the state of all caches to the corresponding remote site if the cache backups to it. " +
               "The remote site will be bring back online",
         name = "pushState"
   )
   public final String pushState(@Parameter(description = "The destination site name", name = "SiteName") String site) {
      return toJMXResponse(performMultiCacheOperation(operations -> operations.pushState(site)));
   }

   @ManagedOperation(
         displayName = "Cancel Push State",
         description = "Cancels the push state on all the caches to remote site.",
         name = "CancelPushState"
   )
   public final String cancelPushState(@Parameter(description = "The destination site name", name = "SiteName") final String site) {
      return toJMXResponse(performMultiCacheOperation(operations -> operations.cancelPushState(site)));
   }

   public final Map<String, SiteStatus> globalStatus() {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
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

   /**
    * Execute an operation for all caches in the site
    * @return A Map keyed by the cache name and with the outcome in the value. Possible values are 'ok' or the
    * error message.
    */
   private Map<String, String> performMultiCacheOperation(Operation operation) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      Collection<CacheXSiteAdminOperation> admOps = collectXSiteAdminOperation();
      if (admOps.isEmpty()) {
         return Collections.emptyMap();
      }
      return admOps.stream().collect(toMap(op -> op.cacheName, op -> {
         try {
            return operation.execute(op.xSiteAdminOperations);
         } catch (Exception e) {
            return "Exception on " + op.cacheName + " : " + e.getMessage();
         }
      }));
   }

   private Collection<CacheXSiteAdminOperation> collectXSiteAdminOperation() {
      Collection<String> cacheNames = cacheManager.getCacheNames();
      List<CacheXSiteAdminOperation> operations = new ArrayList<>(cacheNames.size() + 1);
      for (String cacheName : cacheNames) {
         addCacheAdmin(cacheManager.getCache(cacheName, false), operations);
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
