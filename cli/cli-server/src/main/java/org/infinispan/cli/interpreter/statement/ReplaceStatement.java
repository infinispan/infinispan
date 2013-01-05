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
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

/**
 * Replaces an existing entry in the cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ReplaceStatement implements Statement {
   private static final Log log = LogFactory.getLog(ReplaceStatement.class, Log.class);
   private enum Options { CODEC };
   final KeyData keyData;
   final Object oldValue;
   final Object newValue;
   final Long expires;
   final Long maxIdle;
   final private List<Option> options;

   public ReplaceStatement(final List<Option> options, final KeyData key, final Object newValue, final ExpirationData exp) {
      this(options, key, null, newValue, exp);
   }

   public ReplaceStatement(final List<Option> options, final KeyData key, final Object oldValue, final Object newValue, final ExpirationData exp) {
      this.options = options;
      this.keyData = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
      if (exp != null) {
         this.expires = exp.expires;
         this.maxIdle = exp.maxIdle;
      } else {
         this.expires = null;
         this.maxIdle = null;
      }
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      Codec codec = session.getCodec();
      if (options.size() > 0) {
         for (Option option : options) {
            switch (option.toEnum(Options.class)) {
            case CODEC: {
               if(option.getParameter()==null) {
                  throw log.missingOptionParameter(option.getName());
               } else {
                  codec = session.getCodec(option.getParameter());
               }
               break;
            }
            }
         }
      }
      Object encodedKey = codec.encodeKey(keyData.getKey());
      Object encodedOldValue = codec.encodeValue(oldValue);
      Object encodedNewValue = codec.encodeValue(newValue);
      if (expires == null) {
         if(oldValue!=null) {
            cache.replace(encodedKey, encodedOldValue, encodedNewValue);
         } else {
            cache.replace(encodedKey, encodedNewValue);
         }
      } else if (maxIdle == null) {
         if(oldValue!=null) {
            cache.replace(encodedKey, encodedOldValue, encodedNewValue, expires, TimeUnit.MILLISECONDS);
         } else {
            cache.replace(encodedKey, encodedNewValue, expires, TimeUnit.MILLISECONDS);
         }
      } else {
         if(oldValue!=null) {
            cache.replace(encodedKey, encodedOldValue, encodedNewValue, expires, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS);
         } else {
            cache.replace(encodedKey, encodedNewValue, expires, TimeUnit.MILLISECONDS, maxIdle, TimeUnit.MILLISECONDS);
         }
      }

      return EmptyResult.RESULT;
   }

}
