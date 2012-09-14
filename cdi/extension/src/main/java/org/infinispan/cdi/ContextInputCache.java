/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.cdi;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.mapreduce.Mapper;

/**
 * ContextInputCache keeps track of {@link Input} cache to be injected into Callables from
 * {@link DistributedExecutorService} and {@link Mapper} from {@link MapReduceTask} using CDI
 * mechanism. The cache injected will be the cache used to construct
 * {@link DistributedExecutorService} and/or {@link MapReduceTask}
 * 
 * @author Vladimir Blagoejvic
 * @since 5.2
 * @see InfinispanExtension#registerInputCacheCustomBean(javax.enterprise.inject.spi.AfterBeanDiscovery,
 *      javax.enterprise.inject.spi.BeanManager)
 * 
 */
public class ContextInputCache {

   /*
    * Using thread local was the last choice. See https://issues.jboss.org/browse/ISPN-2181 for more
    * details and design decisions made
    */
   private static ThreadLocal<Cache<?, ?>> contextualCache = new ThreadLocal<Cache<?, ?>>();

   public static <K, V> void set(Cache<K, V> input) {
      contextualCache.set(input);
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Cache<K, V> get() {
      return (Cache<K, V>) contextualCache.get();
   }

   public static void clean() {
      contextualCache.remove();
   }

}
