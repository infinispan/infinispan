package org.infinispan.cli.connection;

import java.io.Closeable;
import java.util.Collection;

import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.Context;

public interface Connection extends Closeable {

   void connect(String credentials) throws Exception;

   boolean needsCredentials();

   void execute(Context context, CommandBuffer commandBuffer);

   String getActiveCache();

   String getActiveContainer();

   Collection<String> getAvailableCaches();

   Collection<String> getAvailableContainers();

   boolean isConnected();

   void setActiveContainer(String name);
}
