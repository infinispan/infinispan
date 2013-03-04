/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.remoting.responses;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Used in Total Order based commit protocol in Distributed Mode
 * <p/>
 * This filter awaits for one valid response for each key to be validated. This way, it avoids waiting for the reply of
 * all nodes involved in the transaction without compromise consistency
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class KeysValidateFilter implements ResponseFilter {

   private static final Log log = LogFactory.getLog(KeysValidateFilter.class);
   private final Address localAddress;
   private final Set<Object> keysAwaitingValidation;
   private boolean selfDelivered;

   public KeysValidateFilter(Address localAddress, Set<Object> keysAwaitingValidation) {
      this.localAddress = localAddress;
      this.keysAwaitingValidation = new HashSet<Object>(keysAwaitingValidation);
      this.selfDelivered = false;
   }

   @Override
   public synchronized boolean isAcceptable(Response response, Address sender) {
      if (sender.equals(localAddress)) {
         selfDelivered = true;
      }
      if (response instanceof SuccessfulResponse) {
         Object retVal = ((SuccessfulResponse) response).getResponseValue();
         if (retVal instanceof Collection<?>) {
            keysAwaitingValidation.removeAll((Collection<?>) retVal);
            if (log.isTraceEnabled()) {
               log.tracef("Received keys validated: %s. Awaiting validation of %s. Self Delivered? %s",
                          retVal, keysAwaitingValidation, selfDelivered);
            }
         } else if (retVal instanceof EntryVersionsMap) {
            keysAwaitingValidation.removeAll(((EntryVersionsMap) retVal).keySet());
            if (log.isTraceEnabled()) {
               log.tracef("Received keys validated: %s. Awaiting validation of %s. Self Delivered? %s",
                          ((EntryVersionsMap) retVal).keySet(), keysAwaitingValidation, selfDelivered);
            }
         } else {
            log.unexpectedResponse(retVal.getClass().toString(), "Collection or EntryVersionMap");
         }
      }
      return true;
   }

   @Override
   public synchronized boolean needMoreResponses() {
      return !selfDelivered || !keysAwaitingValidation.isEmpty();
   }

   public synchronized final boolean isAllKeysValidated() {
      return keysAwaitingValidation.isEmpty();
   }
}
