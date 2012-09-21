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

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class MapReduceMassIndexer implements MassIndexer {

   private final AdvancedCache<Object, Object> cache;

   public MapReduceMassIndexer(AdvancedCache cache, SearchFactoryIntegrator searchFactory) {
      this.cache = cache;
   }

   @Override
   public void start() {
      wipeExistingIndexes();
      new MapReduceTask<Object, Object, Object, LuceneWork>(cache)
         .mappedWith(new IndexingMapper())
         .reducedWith(new IndexingReducer())
         .execute();
   }

   private void wipeExistingIndexes() {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.purgeAllIndexes();
   }

}
