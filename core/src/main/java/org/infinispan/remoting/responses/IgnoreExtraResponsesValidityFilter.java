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
import java.util.Set;

/**
 * A filter that only expects responses from an initial set of targets.
 *
 * Useful when sending a command to {@code null} to ensure we don't wait for responses from
 * cluster members that weren't properly started when the command was sent.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public class IgnoreExtraResponsesValidityFilter implements ResponseFilter {

   private Set<Address> expectedResponses;

   public IgnoreExtraResponsesValidityFilter(Collection<Address> cacheMembers, Address self) {
      this.expectedResponses = new HashSet<Address>(cacheMembers);
      expectedResponses.remove(self);
   }

   public boolean isAcceptable(Response response, Address address) {
      expectedResponses.remove(address);

      // always return true to make sure a response is logged by the JGroups RpcDispatcher.
      return true;
   }

   public boolean needMoreResponses() {
      return !expectedResponses.isEmpty();
   }

}
