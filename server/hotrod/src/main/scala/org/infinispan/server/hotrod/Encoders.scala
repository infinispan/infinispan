package org.infinispan.server.hotrod

import logging.{JavaLog, Log}
import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import collection.JavaConversions._
import org.infinispan.configuration.cache.Configuration
import org.infinispan.distribution.ch.ConsistentHash
import collection.mutable.ArrayBuffer
import collection.immutable.{TreeMap, SortedMap}
import collection.mutable
import org.infinispan.util.logging.LogFactory

/**
 * Version specific encoders are included here.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
object Encoders {

   /**
    * Encoder for version 1.0 of the Hot Rod protocol.
    */
   object Encoder10 extends AbstractEncoder1x

   /**
    * Encoder for version 1.1 of the Hot Rod protocol.
    */
   object Encoder11 extends AbstractTopologyAwareEncoder1x with Log

   /**
    * Encoder for version 1.2 of the Hot Rod protocol.
    */
   object Encoder12 extends AbstractTopologyAwareEncoder1x with Log
}
