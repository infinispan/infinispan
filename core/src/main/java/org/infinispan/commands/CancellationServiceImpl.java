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
package org.infinispan.commands;

import java.util.Map;
import java.util.UUID;

import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * CancellationServiceImpl is a default implementation of {@link CancellationService}
 * 
 * @author Vladimir Blagojevic
 * @since 5.2 
 */
public class CancellationServiceImpl implements CancellationService {

   private static final Log log = LogFactory.getLog(CancellationServiceImpl.class);
   private final Map<UUID, Thread> commandThreadMap = CollectionFactory.makeConcurrentMap();

   @Override
   public void register(Thread t, UUID id) {
      commandThreadMap.put(id, t);
   }

   @Override
   public void unregister(UUID id) {
      commandThreadMap.remove(id);
   }

   @Override
   public void cancel(UUID id) {
      Thread thread = commandThreadMap.get(id);       
      if (thread != null) {
         log.trace("Calling interrupt on thread " + thread);
         thread.interrupt();
      } else{
         log.couldNotInterruptThread(id);
      }
   }
}
