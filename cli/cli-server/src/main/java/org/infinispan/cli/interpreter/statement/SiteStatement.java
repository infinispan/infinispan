/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
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
      Cache<Object, Object> cache = session.getCache(siteData.getCacheName());
      String siteName = siteData.getSiteName();
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
