package org.infinispan.server

import org.infinispan.remoting.transport.Address

/**
 * @author Galder Zamarreño
 */
package object hotrod {

   type Bytes = Array[Byte]
   type Cache = org.infinispan.AdvancedCache[Bytes, Bytes]
   type AddressCache = org.infinispan.Cache[Address, ServerAddress]
   type InternalCacheEntry = org.infinispan.container.entries.InternalCacheEntry[Bytes, Bytes]
   type NamedFactory = Option[(String, List[Bytes])]

}
