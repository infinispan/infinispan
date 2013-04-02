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

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.infinispan.client.hotrod.configuration.ConnectionPoolConfiguration;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class PropsKeyedObjectPoolFactory<K, V> extends GenericKeyedObjectPoolFactory<K, V> {

   public PropsKeyedObjectPoolFactory(KeyedPoolableObjectFactory<K, V> factory, ConnectionPoolConfiguration configuration) {
      super(factory,
            configuration.maxActive(),
            mapExhaustedAction(configuration.exhaustedAction()),
            configuration.maxWait(),
            configuration.maxIdle(),
            configuration.maxTotal(),
            configuration.minIdle(),
            configuration.testOnBorrow(),
            configuration.testOnReturn(),
            configuration.timeBetweenEvictionRuns(),
            configuration.numTestsPerEvictionRun(),
            configuration.minEvictableIdleTime(),
            configuration.testWhileIdle(),
            configuration.lifo());
   }

   private static byte mapExhaustedAction(ExhaustedAction action) {
      switch (action) {
      case CREATE_NEW:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW;
      case EXCEPTION:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL;
      case WAIT:
      default:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
      }
   }
}
