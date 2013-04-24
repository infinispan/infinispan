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
package org.infinispan.remoting;

import org.infinispan.CacheException;

/**
 * Thrown when an RPC problem occurred on the caller.
 *
 * @author (various)
 * @since 4.0
 */
public class RpcException extends CacheException {

   private static final long serialVersionUID = 33172388691879866L;

   public RpcException() {
      super();
   }

   public RpcException(Throwable cause) {
      super(cause);
   }

   public RpcException(String msg) {
      super(msg);
   }

   public RpcException(String msg, Throwable cause) {
      super(msg, cause);
   }
}