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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Performs operation related to rolling upgrades
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class UpgradeStatement implements Statement {
   public static final Log log = LogFactory.getLog(UpgradeStatement.class, Log.class);

   final String cacheName;
   final private List<Option> options;

   public UpgradeStatement(List<Option> options, String cacheName) {
      this.options = options;
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      boolean all = false;
      UpgradeMode mode = UpgradeMode.NONE;
      String migratorName = null;

      for (Option opt : options) {
         if ("all".equals(opt.getName())) {
            all = true;
         } else if ("dumpkeys".equals(opt.getName())) {
            mode = UpgradeMode.DUMPKEYS;
         } else if ("synchronize".equals(opt.getName())) {
            mode = UpgradeMode.SYNCHRONIZE;
            migratorName = opt.getParameter();
            if (migratorName == null) {
               throw log.missingMigrator();
            }
         } else if ("disconnectsource".equals(opt.getName())) {
            mode = UpgradeMode.DISCONNECTSOURCE;
            migratorName = opt.getParameter();
            if (migratorName == null) {
               throw log.missingMigrator();
            }
         } else {
            throw new StatementException("Unknown option " + opt.getName());
         }
      }
      switch (mode) {
      case DUMPKEYS: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            upgradeManager.recordKnownGlobalKeyset();
         }
         break;
      }
      case SYNCHRONIZE: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            try {
               upgradeManager.synchronizeData(migratorName);
            } catch (Exception e) {
               throw new StatementException(e.getMessage());
            }
         }
         break;
      }
      case DISCONNECTSOURCE: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            try {
               upgradeManager.disconnectSource(migratorName);
            } catch (Exception e) {
               throw new StatementException(e.getMessage());
            }
         }
         break;
      }
      case NONE: {
         throw log.missingUpgradeAction();
      }
      }

      return EmptyResult.RESULT;
   }

   private List<Cache<?, ?>> getAllCaches(Session session) {
      List<Cache<?, ?>> caches = new ArrayList<Cache<?, ?>>();
      EmbeddedCacheManager container = session.getCacheManager();
      for (String cacheName : container.getCacheNames()) {
         if (container.isRunning(cacheName)) {
            caches.add(session.getCache(cacheName));
         }
      }
      caches.add(container.getCache());

      return caches;
   }

   private enum UpgradeMode {
      NONE, DUMPKEYS, SYNCHRONIZE, DISCONNECTSOURCE
   }
}
