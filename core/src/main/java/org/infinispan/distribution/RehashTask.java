/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * A task that handles the rehashing of data in the cache system wheh nodes join or leave the cluster.  This abstract
 * class contains common functionality.  Subclasses will specify different behavior for nodes joining and leaving.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class RehashTask implements Callable<Void> {
   protected DistributionManager distributionManager;
   protected RpcManager rpcManager;
   protected Configuration configuration;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected final Address self;
   protected final Log log = LogFactory.getLog(getClass());
   protected final boolean trace = log.isTraceEnabled();


   protected RehashTask(DistributionManagerImpl distributionManager, RpcManager rpcManager,
            Configuration configuration, CommandsFactory cf, DataContainer dataContainer) {
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.self = rpcManager.getAddress();
   }

   public Void call() throws Exception {
      try {
         performRehash();
      }
      catch (InterruptedException e) {
         log.debugf("Rehash was interrupted because the cache is shutting down");
      }
      catch (Throwable th) {
         // there is no one else to handle the exception below us
         log.errorDuringRehash(th);
      }
      return null;
   }

   protected abstract void performRehash() throws Exception;

   protected Collection<Address> coordinator() {
      return Collections.singleton(rpcManager.getTransport().getCoordinator());
   }


}
