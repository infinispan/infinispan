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
