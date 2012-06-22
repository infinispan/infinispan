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
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.upgrade.RollingUpgradeManager;

/**
 * Performs operation related to rolling upgrades
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class UpgradeStatement implements Statement {

   final String cacheName;
   final private List<Option> options;

   public UpgradeStatement(List<Option> options, String cacheName) {
      this.options = options;
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(cacheName);
      RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      for (Option opt : options) {
         if ("dumpkeys".equals(opt.getName())) {
            upgradeManager.recordKnownGlobalKeyset();
         } else {
            throw new StatementException("Unknown option "+opt.getName());
         }
      }

      return EmptyResult.RESULT;
   }

}
