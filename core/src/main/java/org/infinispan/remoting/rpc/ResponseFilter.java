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
package org.infinispan.remoting.rpc;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;

/**
 * A mechanism of filtering RPC responses.  Used with the RPC manager.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ResponseFilter {
   /**
    * Determines whether a response from a given sender should be added to the response list of the request
    *
    * @param response The response (usually a serializable value)
    * @param sender   The sender of response
    * @return True if we should add the response to the response list of a request, otherwise false. In the latter case,
    *         we don't add the response to the response list.
    */
   boolean isAcceptable(Response response, Address sender);

   /**
    * Right after calling {@link #isAcceptable(Response, Address)}, this method is called to see whether we are done
    * with the request and can unblock the caller
    *
    * @return False if the request is done, otherwise true
    */
   boolean needMoreResponses();

}
