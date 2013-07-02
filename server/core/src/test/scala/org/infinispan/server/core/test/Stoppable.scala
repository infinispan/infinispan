package org.infinispan.server.core.test

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
