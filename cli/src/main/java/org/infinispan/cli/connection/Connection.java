package org.infinispan.cli.connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;

public interface Connection extends Closeable {

   void connect() throws IOException;

   void connect(String username, String password) throws IOException;

   String getURI();

   String execute(BiFunction<RestClient, Resource, CompletionStage<RestResponse>> op, ResponseMode responseMode) throws IOException;

   Resource getActiveResource();

   void setActiveResource(Resource resource);

   Resource getActiveContainer();

   Collection<String> getAvailableCaches();

   Collection<String> getAvailableContainers();

   Collection<String> getAvailableCounters() throws IOException;

   Collection<String> getAvailableCacheConfigurations();

   Collection<String> getAvailableSchemas() throws IOException;

   Collection<String> getAvailableServers() throws IOException;

   Collection<String> getAvailableSites(String cache) throws IOException;

   Collection<String> getAvailableTasks() throws IOException;

   Iterable<Map<String, String>> getCacheKeys(String cache) throws IOException;

   Iterable<Map<String, String>> getCacheKeys(String cache, int limit) throws IOException;

   Iterable<Map<String, String>> getCacheEntries(String cache, int limit, boolean metadata) throws IOException;

   Iterable<String> getCounterValue(String counter) throws IOException;

   boolean isConnected();

   String describeContainer() throws IOException;

   String describeCache(String cache) throws IOException;

   String describeKey(String cache, String key) throws IOException;

   String describeConfiguration(String configuration) throws IOException;

   String describeCounter(String counter) throws IOException;

   String describeTask(String taskName) throws IOException;

   String getConnectionInfo();

   String getServerVersion();

   Collection<String> getClusterNodes();

   Collection<String> getAvailableLogAppenders() throws IOException;

   Collection<String> getAvailableLoggers() throws IOException;

   Collection<String> getBackupNames() throws IOException;

   Collection<String> getSitesView();

   String getLocalSiteName();

   boolean isRelayNode();

   Collection<String> getRelayNodes();

   Collection<String> getConnectorNames() throws IOException;

   MediaType getEncoding();

   void setEncoding(MediaType encoding);

   void refreshServerInfo() throws IOException;

   Collection<String> getDataSourceNames() throws IOException;

   Collection<String> getCacheConfigurationAttributes(String name);

   String getUsername();

   Collection<String> getRoles() throws IOException;

   enum ResponseMode {QUIET, BODY, FILE, HEADERS}
}
