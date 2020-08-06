package org.infinispan.cli.connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.resources.Resource;

public interface Connection extends Closeable {
   void connect() throws IOException;

   void connect(String username, String password) throws IOException;

   /**
    * Executes the supplied commands
    *
    * @param commands
    * @return the output.
    */
   String execute(List<CommandInputLine> commands) throws IOException;

   Resource getActiveResource();

   Resource getActiveContainer();

   Collection<String> getAvailableCaches(String container);

   Collection<String> getAvailableContainers();

   Collection<String> getAvailableCounters(String container) throws IOException;

   Collection<String> getAvailableCacheConfigurations(String container);

   Collection<String> getAvailableSchemas(String container) throws IOException;

   Collection<String> getAvailableServers(String container) throws IOException;

   Collection<String> getAvailableSites(String container, String cache) throws IOException;

   Collection<String> getAvailableTasks(String container) throws IOException;

   Iterable<String> getCacheKeys(String container, String cache) throws IOException;

   Iterable<String> getCounterValue(String container, String counter) throws IOException;

   boolean isConnected();

   String describeContainer(String container) throws IOException;

   String describeCache(String container, String cache) throws IOException;

   String describeKey(String container, String cache, String key) throws IOException;

   String describeConfiguration(String container, String configuration) throws IOException;

   String describeCounter(String container, String counter) throws IOException;

   String describeTask(String container, String taskName) throws IOException;

   String getConnectionInfo();

   String getServerVersion();

   Collection<String> getClusterNodes();

   Collection<String> getAvailableLogAppenders() throws IOException;

   Collection<String> getAvailableLoggers() throws IOException;

   Collection<String> getBackupNames(String container) throws IOException;
}
