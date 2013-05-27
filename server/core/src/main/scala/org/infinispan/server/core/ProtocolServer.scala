/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.server.core

import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.ChannelHandler
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import org.infinispan.server.core.transport.LifecycleChannelPipelineFactory

/**
 * Represents a protocol compliant server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {
   type SuitableConfiguration <: ProtocolServerConfiguration

   /**
    * Starts the server backed by the given cache manager and with the corresponding configuration.
    */
   def start(configuration: SuitableConfiguration, cacheManager: EmbeddedCacheManager)

   /**
    * Starts the server backed by the given cache manager and with the corresponding properties. If properties object
    * is either null or empty, default values depending on the server type are assumed. Note that properties mandate
    * String keys and values. Accepted property keys and default values are listed in {@link Main} class.
    */
   @Deprecated
   def startWithProperties(properties: Properties, cacheManager: EmbeddedCacheManager)

   /**
    *  Stops the server
    */
   def stop

   /**
    * Gets the encoder for this protocol server. The encoder is responsible for writing back common header responses
    * back to client. This method can return null if the server has no encoder. You can find an example of the server
    * that has no encoder in the Memcached server.
    */
   def getEncoder: OneToOneEncoder

   /**
    * Gets the decoder for this protocol server. The decoder is responsible for reading client requests.
    * This method cannot return null.
    */
   def getDecoder: ChannelHandler

   /**
    * Returns the configuration used to start this server
    */
   def getConfiguration: SuitableConfiguration

   /**
    * Returns a pipeline factory
    */
   def getPipeline: LifecycleChannelPipelineFactory
}
