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
package org.infinispan.server.core.transport

import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration
import javax.net.ssl.SSLEngine
import org.infinispan.util.SslContextFactory

/**
 * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
 * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
 * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelPipelineFactory(server: ProtocolServer,
                                  encoder: ChannelDownstreamHandler)
      extends LifecycleChannelPipelineFactory {

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      val ssl = server.getConfiguration.ssl
      if (ssl.enabled())
         pipeline.addLast("ssl", new SslHandler(createSslEngine(ssl)))
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)

      return pipeline;
   }

   override def stop {
      // No-op
   }

   def createSslEngine(ssl: SslConfiguration): SSLEngine = {
      val sslContext = if (ssl.sslContext != null) {
         ssl.sslContext
      } else {
         SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.trustStoreFileName, ssl.trustStorePassword)
      }
      SslContextFactory.getEngine(sslContext, false, ssl.requireClientAuth)
   }
}
