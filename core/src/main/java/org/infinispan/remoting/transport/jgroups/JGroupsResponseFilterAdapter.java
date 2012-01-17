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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Acts as a bridge between JGroups RspFilter and {@link org.infinispan.remoting.rpc.ResponseFilter}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class JGroupsResponseFilterAdapter implements RspFilter {

   final ResponseFilter r;

   /**
    * Creates an instance of the adapter
    *
    * @param r response filter to wrap
    */
   public JGroupsResponseFilterAdapter(ResponseFilter r) {
      this.r = r;
   }

   public boolean isAcceptable(Object response, Address sender) {
      if (response instanceof Exception)
         response = new ExceptionResponse((Exception) response);
      else if (response instanceof Throwable)
         response = new ExceptionResponse(new RuntimeException((Throwable)response));

      return r.isAcceptable((Response) response, JGroupsTransport.fromJGroupsAddress(sender));
   }

   public boolean needMoreResponses() {
      return r.needMoreResponses();
   }
}
