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

import org.infinispan.config.Configuration;

/**
 * Represents different handling mechanisms when dealing with remote command responses. 
 * These include waiting for responses from all nodes in the cluster ({@link ResponseMode#SYNCHRONOUS}}),
 * not waiting for any responses at all ({@link ResponseMode#ASYNCHRONOUS}} or 
 * {@link ResponseMode#ASYNCHRONOUS_WITH_SYNC_MARSHALLING}}), or waiting for first valid response 
 * ({@link ResponseMode#WAIT_FOR_VALID_RESPONSE}})
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum ResponseMode {
   SYNCHRONOUS,
   SYNCHRONOUS_IGNORE_LEAVERS,
   ASYNCHRONOUS,
   ASYNCHRONOUS_WITH_SYNC_MARSHALLING,
   WAIT_FOR_VALID_RESPONSE;

   public boolean isAsynchronous() {
      return this == ASYNCHRONOUS || this == ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
   }

   public static ResponseMode getAsyncResponseMode(Configuration c) {
      return c.isUseAsyncMarshalling() ? ResponseMode.ASYNCHRONOUS : ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
   }

}
