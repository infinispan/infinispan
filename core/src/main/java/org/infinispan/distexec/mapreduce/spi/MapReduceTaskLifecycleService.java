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
package org.infinispan.distexec.mapreduce.spi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class MapReduceTaskLifecycleService {

   private static final Log log = LogFactory.getLog(MapReduceTaskLifecycleService.class);
   private static MapReduceTaskLifecycleService service;
   private List<MapReduceTaskLifecycle> lifecycles;

   private MapReduceTaskLifecycleService() {
      ServiceLoader<MapReduceTaskLifecycle> loader = ServiceLoader.load(MapReduceTaskLifecycle.class);
      lifecycles = new ArrayList<MapReduceTaskLifecycle>();
      for (MapReduceTaskLifecycle l : loader) {
         lifecycles.add(l);
      }
   }

   public static synchronized MapReduceTaskLifecycleService getInstance() {
      if (service == null) {
         service = new MapReduceTaskLifecycleService();
      }
      return service;
   }

   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPreExecute(mapper);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPostExecute(mapper);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPreExecute(reducer);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPostExecute(reducer);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }
}