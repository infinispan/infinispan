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

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

public class PutStatement implements Statement {
   final KeyData keyData;
   final Object value;
   final Long expires;
   final Long maxIdle;

   public PutStatement(final KeyData key, final Object value, final ExpirationData exp) {
      this.keyData = key;
      this.value = value;
      if (exp != null) {
         this.expires = exp.expires;
         this.maxIdle = exp.maxIdle;
      } else {
         this.expires = null;
         this.maxIdle = null;
      }
   }

   @Override
   public Result execute(Session session) {
      Cache<Object, Object> cache;
      if (keyData.getCacheName()!=null) {
         cache = (Cache<Object, Object>) session.getCache(keyData.getCacheName());
      } else {
         cache = (Cache<Object, Object>) session.getCache();
      }
      if (expires == null) {
         cache.put(keyData.getKey(), value);
      } else if (maxIdle == null) {
         cache.put(keyData.getKey(), value, expires, TimeUnit.MILLISECONDS);
      } else {
         cache.put(keyData.getKey(), value, expires, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS);
      }

      return EmptyResult.RESULT;
   }

}
