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

import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycle;

/**
 * Initializes the custom Map Reduce tasks we use to rebuild indexes
 *  
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class MapReduceInitializer implements MapReduceTaskLifecycle {

   @Override
   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
      if (mapper instanceof IndexingMapper) {
         IndexingMapper im = (IndexingMapper) mapper;
         im.initialize(inputCache);
      }
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      //nothing to do
   }

   @Override
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
      if (reducer instanceof IndexingReducer) {
         IndexingReducer im = (IndexingReducer) reducer;
         im.initialize(inputCache);
      }
   }

   @Override
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
    //nothing to do
   }

}
