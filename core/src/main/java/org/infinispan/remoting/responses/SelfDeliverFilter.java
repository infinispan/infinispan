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

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

/**
 * Used in Total Order based protocol.
 * <p/>
 * This filter awaits until the command is deliver and processed by the local node.
 * <p/>
 * Warning: Non-Total Order command are not self delivered
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class SelfDeliverFilter implements ResponseFilter {

   private final Address localAddress;
   private boolean selfDelivered;

   public SelfDeliverFilter(Address localAddress) {
      this.localAddress = localAddress;
      this.selfDelivered = false;
   }

   @Override
   public boolean isAcceptable(Response response, Address sender) {
      if (sender.equals(localAddress)) {
         selfDelivered = true;
      }
      return true;
   }

   @Override
   public boolean needMoreResponses() {
      return !selfDelivered;
   }
}
