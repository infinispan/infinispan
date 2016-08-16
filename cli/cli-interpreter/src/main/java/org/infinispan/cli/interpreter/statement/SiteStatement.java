package org.infinispan.cli.interpreter.statement;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * Performs operation related to Cross-Site Replication
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SiteStatement implements Statement {
   private static final Log log = LogFactory.getLog(SiteStatement.class, Log.class);

   enum Options {
      OFFLINE, ONLINE, STATUS, PUSH, CANCELPUSH, CANCELRECEIVE, PUSHSTATUS, CLEARPUSHSTATUS, SENDINGSITE,
      ONLINEALL, OFFLINEALL, PUSHALL, CANCELPUSHALL
   }

   final private SiteData siteData;
   final private List<Option> options;

   public SiteStatement(List<Option> options, SiteData siteData) {
      this.options = options;
      this.siteData = siteData;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      if (options.isEmpty()) {
         return EmptyResult.RESULT;
      }
      Options option = options.get(0).toEnum(Options.class);
      if (isGlobalOption(option)) {
         return executeContainerOperation(option, session);
      } else {
         return executeCacheOperation(option, session);
      }

   }

   private Result executeCacheOperation(Options option, Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(siteData != null ? siteData.getCacheName() : null);
      String siteName = siteData != null ? siteData.getSiteName() : null;
      XSiteAdminOperations xsiteAdmin = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);

      requireXSiteAdmin(xsiteAdmin, cache.getName());

      switch (option) {
         case STATUS: {
            return new StringResult(siteName == null ? xsiteAdmin.status() : xsiteAdmin.siteStatus(siteName));
         }
         case ONLINE: {
            requireSiteName(siteName);
            return new StringResult(xsiteAdmin.bringSiteOnline(siteName));
         }
         case OFFLINE: {
            requireSiteName(siteName);
            return new StringResult(xsiteAdmin.takeSiteOffline(siteName));
         }
         case PUSH: {
            requireSiteName(siteName);
            return new StringResult(xsiteAdmin.pushState(siteName));
         }
         case CANCELPUSH: {
            requireSiteName(siteName);
            return new StringResult(xsiteAdmin.cancelPushState(siteName));
         }
         case CANCELRECEIVE: {
            requireSiteName(siteName);
            return new StringResult(xsiteAdmin.cancelReceiveState(siteName));
         }
         case PUSHSTATUS: {
            return new StringResult(prettyPrintMap(xsiteAdmin.getPushStateStatus()));
         }
         case CLEARPUSHSTATUS: {
            return new StringResult(xsiteAdmin.clearPushStateStatus());
         }
         case SENDINGSITE: {
            return new StringResult(xsiteAdmin.getSendingSiteName());
         }
         default:
            return EmptyResult.RESULT;
      }
   }

   private Result executeContainerOperation(Options option, Session session) throws StatementException {
      EmbeddedCacheManager cacheManager = session.getCacheManager();
      GlobalXSiteAdminOperations xSiteAdmin = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
      String siteName = siteData != null ? siteData.getSiteName() : null;
      requireSiteName(siteName);

      switch (option) {
         case OFFLINEALL: {
            return new StringResult(xSiteAdmin.takeSiteOffline(siteName));
         }
         case ONLINEALL: {
            return new StringResult(xSiteAdmin.bringSiteOnline(siteName));
         }
         case PUSHALL: {
            return new StringResult(xSiteAdmin.pushState(siteName));
         }
         case CANCELPUSHALL: {
            return new StringResult(xSiteAdmin.cancelPushState(siteName));
         }
         default:
            return EmptyResult.RESULT;
      }
   }

   private static boolean isGlobalOption(Options option) {
      switch (option) {
         case OFFLINEALL:
         case ONLINEALL:
         case PUSHALL:
         case CANCELPUSHALL:
            return true;
         default:
            return false;
      }
   }

   private static void requireXSiteAdmin(XSiteAdminOperations xSiteAdminOperations, String cacheName) throws StatementException {
      if (xSiteAdminOperations == null) {
         throw log.noBackupsForCache(cacheName);
      }
   }

   private static void requireSiteName(String siteName) throws StatementException {
      if (siteName == null) {
         throw log.siteNameNotSpecified();
      }
   }

   private static String prettyPrintMap(Map<?, ?> map) {
      if (map.isEmpty()) {
         return "";
      }
      StringBuilder builder = new StringBuilder();
      for (Iterator<? extends Map.Entry<?, ?>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<?, ?> entry = iterator.next();
         builder.append(entry.getKey()).append("=").append(entry.getValue());
         if (iterator.hasNext()) {
            builder.append(System.lineSeparator());
         }
      }
      return builder.toString();
   }

}
