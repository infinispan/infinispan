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
package org.infinispan.query.impl.massindex;

import java.util.Iterator;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.util.TimeService;

/**
 * This Reduce doesn't really index the entries but forwards them to the
 * appropriate index master; the backend knows how to deal with sharding
 * and this way we avoid unnecessary round trips.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public final class IndexingReducer implements Reducer<Object, LuceneWork> {

   private DefaultMassIndexerProgressMonitor progressMonitor;
   private DefaultBatchBackend defaultBatchBackend;

   public void initialize(Cache<?, ?> inputCache) {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(inputCache);
      SearchFactoryIntegrator searchFactory = queryInterceptor.getSearchFactory();
      this.progressMonitor = new DefaultMassIndexerProgressMonitor(inputCache.getAdvancedCache().getComponentRegistry()
                                                                         .getTimeService());
      this.defaultBatchBackend = new DefaultBatchBackend(searchFactory, progressMonitor);
   }

   @Override
   public LuceneWork reduce(Object reducedKey, Iterator<LuceneWork> iter) {
      try {
         while (iter.hasNext()) {
            defaultBatchBackend.enqueueAsyncWork(iter.next());
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return null;
   }

}
