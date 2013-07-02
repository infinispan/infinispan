package org.infinispan.server.core.transport

import org.jboss.netty.channel.ChannelPipelineFactory

/**
 * A {@link ChannelPipelineFactory} which might want to perform special operations when stopped
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
abstract class LifecycleChannelPipelineFactory extends ChannelPipelineFactory {

   def stop()
}
