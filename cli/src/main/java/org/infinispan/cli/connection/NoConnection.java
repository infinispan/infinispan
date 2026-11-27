package org.infinispan.cli.connection;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;

public final class NoConnection implements Connection {
   public static final NoConnection INSTANCE = new NoConnection();

   @Override
   public void connect() throws IOException {
   }

   @Override
   public void connect(String username, String password) throws IOException {
   }

   @Override
   public String getURI() {
      return "";
   }

   @Override
   public String execute(BiFunction<RestClient, Resource, CompletionStage<RestResponse>> op, ResponseMode responseMode) throws IOException {
      return "";
   }

   @Override
   public Resource getActiveResource() {
      return null;
   }

   @Override
   public void setActiveResource(Resource resource) {

   }

   @Override
   public Resource getActiveContainer() {
      return null;
   }

   @Override
   public Collection<String> getAvailableCaches() {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableContainers() {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableCounters() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableCacheConfigurations() {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableSchemas() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableServers() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableSites(String cache) throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableTasks() throws IOException {
      return List.of();
   }

   @Override
   public Iterable<Map<String, String>> getCacheKeys(String cache) throws IOException {
      return null;
   }

   @Override
   public Iterable<Map<String, String>> getCacheKeys(String cache, int limit) throws IOException {
      return null;
   }

   @Override
   public Iterable<Map<String, String>> getCacheEntries(String cache, int limit, boolean metadata) throws IOException {
      return null;
   }

   @Override
   public Iterable<String> getCounterValue(String counter) throws IOException {
      return null;
   }

   @Override
   public boolean isConnected() {
      return false;
   }

   @Override
   public String describeContainer() throws IOException {
      return "";
   }

   @Override
   public String describeCache(String cache) throws IOException {
      return "";
   }

   @Override
   public String describeKey(String cache, String key) throws IOException {
      return "";
   }

   @Override
   public String describeConfiguration(String configuration) throws IOException {
      return "";
   }

   @Override
   public String describeCounter(String counter) throws IOException {
      return "";
   }

   @Override
   public String describeTask(String taskName) throws IOException {
      return "";
   }

   @Override
   public String getConnectionInfo() {
      return "";
   }

   @Override
   public String getServerVersion() {
      return "";
   }

   @Override
   public Collection<String> getClusterNodes() {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableLogAppenders() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getAvailableLoggers() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getBackupNames() throws IOException {
      return List.of();
   }

   @Override
   public Collection<String> getSitesView() {
      return List.of();
   }

   @Override
   public String getLocalSiteName() {
      return "";
   }

   @Override
   public boolean isRelayNode() {
      return false;
   }

   @Override
   public Collection<String> getRelayNodes() {
      return List.of();
   }

   @Override
   public Collection<String> getConnectorNames() throws IOException {
      return List.of();
   }

   @Override
   public void credentials(String username, String password) {

   }

   @Override
   public MediaType getEncoding() {
      return null;
   }

   @Override
   public void setEncoding(MediaType encoding) {

   }

   @Override
   public void refreshServerInfo() {

   }

   @Override
   public Collection<String> getDataSourceNames() {
      return List.of();
   }

   @Override
   public Collection<String> getCacheConfigurationAttributes(String name) {
      return List.of();
   }

   @Override
   public String getUsername() {
      return "";
   }

   @Override
   public Collection<String> getRoles() throws IOException {
      return List.of();
   }

   @Override
   public void close() throws IOException {
   }
}
