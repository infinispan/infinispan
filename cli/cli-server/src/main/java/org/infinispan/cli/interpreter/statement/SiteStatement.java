package org.infinispan.cli.interpreter.statement;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * Performs operation related to Cross-Site Replication
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SiteStatement implements Statement {
   private static final Log log = LogFactory.getLog(SiteStatement.class, Log.class);

   static enum Options {
      OFFLINE, ONLINE, STATUS,
   };

   final private SiteData siteData;
   final private List<Option> options;

   public SiteStatement(List<Option> options, SiteData siteData) {
      this.options = options;
      this.siteData = siteData;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(siteData != null ? siteData.getCacheName() : null);
      String siteName = siteData != null ? siteData.getSiteName() : null;
      XSiteAdminOperations xsiteAdmin = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
      for (Option opt : options) {
         switch (opt.toEnum(Options.class)) {
         case STATUS: {
            String status = siteName == null ? xsiteAdmin.status() : xsiteAdmin.siteStatus(siteName);
            return new StringResult(status);
         }
         case ONLINE: {
            if (siteName != null) {
               return new StringResult(xsiteAdmin.bringSiteOnline(siteName));
            } else {
               throw log.siteNameNotSpecified();
            }
         }
         case OFFLINE: {
            if (siteName != null) {
               return new StringResult(xsiteAdmin.takeSiteOffline(siteName));
            } else {
               throw log.siteNameNotSpecified();
            }
         }
         }
      }

      return EmptyResult.RESULT;
   }

}
