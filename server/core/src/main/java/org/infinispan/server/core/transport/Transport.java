package org.infinispan.server.core.transport;

import java.util.concurrent.CompletionStage;

import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelMatcher;

/**
 * Server transport abstraction
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Transport {

   void start();

   void stop();

   boolean isRunning();

   long getTotalBytesWritten();

   long getTotalBytesRead();

   String getHostName();

   int getPort();

   int getNumberIOThreads();

   int getIdleTimeout();

   int getPendingTasks();

   boolean getTcpNoDelay();

   int getSendBufferSize();

   int getReceiveBufferSize();

   int getNumberOfLocalConnections();

   int getNumberOfGlobalConnections();

   CompletionStage<Void> closeChannels(ChannelMatcher channelMatcher);

   ChannelGroup getAcceptedChannels();
}
