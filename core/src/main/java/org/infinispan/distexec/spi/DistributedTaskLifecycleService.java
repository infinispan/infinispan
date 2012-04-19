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
package org.infinispan.distexec.spi;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class DistributedTaskLifecycleService {

   private static final Log log = LogFactory.getLog(DistributedTaskLifecycleService.class);
   private static DistributedTaskLifecycleService service;
   private ServiceLoader<DistributedTaskLifecycle> loader;

   private DistributedTaskLifecycleService() {
      loader = ServiceLoader.load(DistributedTaskLifecycle.class);
   }

   public static synchronized DistributedTaskLifecycleService getInstance() {
      if (service == null) {
         service = new DistributedTaskLifecycleService();
      }
      return service;
   }

   public <T> void onPreExecute(Callable<T> task) {
      try {
         Iterator<DistributedTaskLifecycle> i = loader.iterator();
         while (i.hasNext()) {
            DistributedTaskLifecycle cl = i.next();
            cl.onPreExecute(task);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ", serviceError));
      }
   }

   public <T> void onPostExecute(Callable<T> task) {
      try {
         Iterator<DistributedTaskLifecycle> i = loader.iterator();
         while (i.hasNext()) {
            DistributedTaskLifecycle cl = i.next();
            cl.onPostExecute(task);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ", serviceError));
      }
   }
}