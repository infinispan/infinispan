/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
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

package org.infinispan.xsite.offline;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class DelegatingTransport implements Transport {

   private final Transport actual;

   public DelegatingTransport(Transport actual) {
      this.actual = actual;
   }

   volatile boolean fail;

   @Override
   public BackupResponse backupRemotely(final Collection<XSiteBackup> backups, ReplicableCommand rpcCommand) throws Exception {
      return new BackupResponse() {

         final long creationTime = System.currentTimeMillis();

         @Override
         public void waitForBackupToFinish() throws Exception {
         }

         @Override
         public Map<String, Throwable> getFailedBackups() {
            if (fail) {
               Map<String, Throwable> result = new HashMap<String, Throwable>();
               for (XSiteBackup xSiteBackup : backups) {
                  result.put(xSiteBackup.getSiteName(), new TimeoutException());
               }
               return result;
            } else {
               return Collections.emptyMap();
            }
         }

         @Override
         public Set<String> getCommunicationErrors() {
            if (fail) {
               return Collections.singleton("NYC");
            } else {
               return Collections.emptySet();
            }
         }

         @Override
         public long getSendTimeMillis() {
            return creationTime;
         }

         @Override
         public boolean isEmpty() {
            return false;
         }
      };
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue,responseFilter, totalOrder, anycast);
   }

   @Override
   public boolean isCoordinator() {
      return actual.isCoordinator();
   }

   @Override
   public Address getCoordinator() {
      return actual.getCoordinator();
   }

   @Override
   public Address getAddress() {
      return actual.getAddress();
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return actual.getPhysicalAddresses();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public boolean isMulticastCapable() {
      return actual.isMulticastCapable();
   }

   @Override
   public void start() {
      actual.start();
   }

   @Override
   public void stop() {
      actual.stop();
   }

   @Override
   public int getViewId() {
      return actual.getViewId();
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }

   @Override
   public void checkTotalOrderSupported(boolean anycast) {
      actual.checkTotalOrderSupported(anycast);
   }
}
