package org.infinispan.server.core.test

/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

import org.infinispan.server.core.logging.Log
import org.infinispan.server.core.AbstractProtocolServer

/**
 * Infinispan servers testing util
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
object ServerTestingUtil extends Log {

   def killServer(server: AbstractProtocolServer) {
      try {
         if (server != null) server.stop
      } catch {
         case t: Throwable => error("Error stopping server", t)
      }
   }

}
