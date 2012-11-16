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
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.cli.interpreter.logging.Messages.MSG;

/**
 * Performs operation related to rolling upgrades
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class UpgradeStatement implements Statement {
   private static final Log log = LogFactory.getLog(UpgradeStatement.class, Log.class);

   private enum Options {
      ALL, DUMPKEYS, SYNCHRONIZE, DISCONNECTSOURCE
   };

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
         switch (opt.toEnum(Options.class)) {
         case ALL: {
            all = true;
            break;
         }
         case DUMPKEYS: {
            mode = UpgradeMode.DUMPKEYS;
            break;
         }
         case SYNCHRONIZE: {
            mode = UpgradeMode.SYNCHRONIZE;
            migratorName = opt.getParameter();
            if (migratorName == null) {
               throw log.missingMigrator();
            }
            break;
         }
         case DISCONNECTSOURCE: {
            mode = UpgradeMode.DISCONNECTSOURCE;
            migratorName = opt.getParameter();
            if (migratorName == null) {
               throw log.missingMigrator();
            }
         }
         }
      }
      StringBuilder sb = new StringBuilder();
      switch (mode) {
      case DUMPKEYS: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            upgradeManager.recordKnownGlobalKeyset();
            sb.append(MSG.dumpedKeys(cache.getName()));
            sb.append("\n");
         }
         break;
      }
      case SYNCHRONIZE: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            try {
               long count = upgradeManager.synchronizeData(migratorName);
               sb.append(MSG.synchronizedEntries(count, migratorName, cache.getName()));
               sb.append("\n");
            } catch (Exception e) {
               throw log.dataSynchronizationError(e, migratorName, cache.getName());
            }
         }
         break;
      }
      case DISCONNECTSOURCE: {
         for (Cache<?, ?> cache : all ? getAllCaches(session) : Collections.singletonList(session.getCache(cacheName))) {
            RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
            try {
               upgradeManager.disconnectSource(migratorName);
               sb.append(MSG.disonnectedSource(migratorName, cache.getName()));
               sb.append("\n");
            } catch (Exception e) {
               throw log.sourceDisconnectionError(e, migratorName, cache.getName());
            }
         }
         break;
      }
      default: {
         throw log.missingUpgradeAction();
      }
      }
      return new StringResult(sb.toString());
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
