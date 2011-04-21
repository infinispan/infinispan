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
package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;

/**
 * A filter that tests the validity of {@link org.infinispan.commands.remote.ClusteredGetCommand}s.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ClusteredGetResponseValidityFilter implements ResponseFilter {

   private int numValidResponses = 0;

   private Collection<Address> pendingResponders;

   public ClusteredGetResponseValidityFilter(Collection<Address> pendingResponders) {
      this.pendingResponders = new HashSet<Address>(pendingResponders);
   }

   public boolean isAcceptable(Response response, Address address) {
      pendingResponders.remove(address);

      if (response instanceof SuccessfulResponse) numValidResponses++;

      // always return true to make sure a response is logged by the JGroups RpcDispatcher.
      return true;
   }

   public boolean needMoreResponses() {
      return numValidResponses < 1 && !pendingResponders.isEmpty();
   }

}
