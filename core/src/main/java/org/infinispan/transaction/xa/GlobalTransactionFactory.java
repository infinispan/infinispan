/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.transaction.xa;

import org.infinispan.config.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;

import java.util.Random;

/**
 * Factory for GlobalTransaction/DeadlockDetectingGlobalTransaction.
 *
 * @author Mircea.Markus@jboss.com
 */
public class GlobalTransactionFactory {

   private boolean isEddEnabled = false;

   private DistributionManager distributionManager;

   private Configuration configuration;

   @Inject
   public void init(DistributionManager distributionManager, Configuration configuration) {
      this.distributionManager = distributionManager;
      this.configuration = configuration;
   }

   /** this class is internally synchronized, so it can be shared between instances */
   private final Random rnd = new Random();

   private long generateRandomId() {
      return rnd.nextLong();
   }


   public GlobalTransactionFactory() {
   }

   public GlobalTransactionFactory(boolean eddEnabled) {
      isEddEnabled = eddEnabled;
   }

   @Inject
   public void init(Configuration configuration) {
      isEddEnabled = configuration.isEnableDeadlockDetection();
   }

   @Start
   public void start() {

   }

   public GlobalTransaction instantiateGlobalTransaction() {
      if (isEddEnabled) {
         return new DldGlobalTransaction();
      } else {
         return new GlobalTransaction();
      }
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      GlobalTransaction gtx;
      if (isEddEnabled) {
         DldGlobalTransaction globalTransaction;
         globalTransaction = new DldGlobalTransaction(addr, remote);
         globalTransaction.setCoinToss(generateRandomId());
         gtx = globalTransaction;
      } else {
         gtx = new GlobalTransaction(addr, remote);
      }
      return gtx;
   }
}
