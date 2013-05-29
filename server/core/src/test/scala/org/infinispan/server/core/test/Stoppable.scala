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

import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.TestingUtil
import org.infinispan.server.core.AbstractProtocolServer

/**
 * Stoppable implements simple wrappers for objects which need to be stopped in certain way after being used
 * @author Galder Zamarreño
 */
class Stoppable {
   // Empty - do not delete!
}

object Stoppable {

   def useCacheManager[T <: EmbeddedCacheManager](stoppable: T)
           (block: T => Unit) {
      try {
         block(stoppable)
      } finally {
         TestingUtil.killCacheManagers(stoppable)
      }
   }

   def useServer[T <: AbstractProtocolServer](stoppable: T)
           (block: T => Unit) {
      try {
         block(stoppable)
      } finally {
         ServerTestingUtil.killServer(stoppable)
      }
   }

}
