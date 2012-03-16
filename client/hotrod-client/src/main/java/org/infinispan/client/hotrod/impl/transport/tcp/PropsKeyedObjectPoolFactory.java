/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod.impl.transport.tcp;

import java.util.Properties;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class PropsKeyedObjectPoolFactory extends GenericKeyedObjectPoolFactory {


   private static final Log log = LogFactory.getLog(PropsKeyedObjectPoolFactory.class);

   public PropsKeyedObjectPoolFactory(KeyedPoolableObjectFactory factory, Properties props) {
      super(factory);
      _maxActive = intProp(props, "maxActive", -1);
      _maxTotal = intProp(props, "maxTotal", -1);
      _maxIdle = intProp(props, "maxIdle", -1);
      _whenExhaustedAction = (byte) intProp(props, "whenExhaustedAction", (int) GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
      _testOnBorrow = booleanProp(props, "testOnBorrow", false);
      _testOnReturn = booleanProp(props, "testOnReturn", false);
      _timeBetweenEvictionRunsMillis = intProp(props, "timeBetweenEvictionRunsMillis", 2 * 60 * 1000);
      _minEvictableIdleTimeMillis = longProp(props, "minEvictableIdleTimeMillis", 5 * 60 * 1000);
      _numTestsPerEvictionRun = intProp(props, "numTestsPerEvictionRun", 3);
      _testWhileIdle = booleanProp(props, "testWhileIdle", true);
      _minIdle = intProp(props, "minIdle", 1);
      _lifo = booleanProp(props, "lifo", true);
   }

   private int intProp(Properties p, String name, int defaultValue) {
      return (Integer) getValue(p, name, defaultValue);
   }

   private boolean booleanProp(Properties p, String name, Boolean defaultValue) {
      return (Boolean) getValue(p, name, defaultValue);
   }

   private long longProp(Properties p, String name, long defaultValue) {
      return (Long) getValue(p, name, defaultValue);
   }

   public Object getValue(Properties p, String name, Object defaultValue) {
      Object propValue = p.get(name);
      if (propValue == null) {
         log.tracef("%s property not specified, using default value (%s)", name, defaultValue);
         return defaultValue;
      } else {
         log.tracef("%s = %s", name, propValue);
         if (defaultValue instanceof Integer) {
            return Integer.parseInt(propValue.toString());
         } else if (defaultValue instanceof Boolean) {
            return Boolean.parseBoolean(propValue.toString());
         } else if (defaultValue instanceof Long) {
            return Long.parseLong(propValue.toString());
         } else {
            throw new IllegalStateException();
         }
      }
   }
}

