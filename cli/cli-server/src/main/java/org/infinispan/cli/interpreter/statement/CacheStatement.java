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

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * CacheStatement shows the currently selected cache or selects a cache to be used as default for CLI operations
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CacheStatement implements Statement {
   private static final Log log = LogFactory.getLog(CacheStatement.class, Log.class);

   final String cacheName;

   public CacheStatement(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      if (cacheName != null) {
         session.setCurrentCache(cacheName);
         return EmptyResult.RESULT;
      } else {
         Cache<?, ?> currentCache = session.getCurrentCache();
         if (currentCache != null) {
            return new StringResult(currentCache.getName());
         } else {
            throw log.noCacheSelectedYet();
         }
      }
   }

}
