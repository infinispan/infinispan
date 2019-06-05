package org.infinispan.server.core.transport;

/**
 * Server transport abstraction
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface Transport {

   void start();

   void stop();

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

}
