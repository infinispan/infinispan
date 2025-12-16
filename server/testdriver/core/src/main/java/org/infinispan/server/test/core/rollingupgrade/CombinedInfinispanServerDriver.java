package org.infinispan.server.test.core.rollingupgrade;

import java.io.Closeable;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import javax.management.MBeanServerConnection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;

import net.spy.memcached.ConnectionFactoryBuilder;

/**
 * Wrapper Driver class takes two drivers, where the from driver may have some servers that are stopped. For each
 * stopped server the to driver must have a running server to take its place. This allows for seamless retrieval
 * of address information to connect to the drivers.
 * <p>
 * No lifecycle methods are allowed to be called on this driver and should be handled on the individual drivers
 * separately.
 */
public class CombinedInfinispanServerDriver implements InfinispanServerDriver {
   private final ContainerInfinispanServerDriver fromDriver;
   private final ContainerInfinispanServerDriver toDriver;

   public CombinedInfinispanServerDriver(ContainerInfinispanServerDriver fromDriver, ContainerInfinispanServerDriver toDriver) {
      this.fromDriver = fromDriver;
      this.toDriver = toDriver;
   }

   @Override
   public InetAddress getTestHostAddress() {
      return fromDriver.getTestHostAddress();
   }

   @Override
   public void prepare(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void start(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public File getCertificateFile(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public File getRootDir() {
      throw new UnsupportedOperationException();
   }

   @Override
   public File getConfDir() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyKeyStore(ConfigurationBuilder builder, String certificateName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyKeyStore(ConfigurationBuilder builder, String certificateName, String type, String provider) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(ConfigurationBuilder builder, String certificateName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(ConfigurationBuilder builder, String certificateName, String type, String provider) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(ConnectionFactoryBuilder builder, String certificateName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void applyTrustStore(ConnectionFactoryBuilder builder, String certificateName, String type, String provider) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void pause(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void resume(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void kill(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void restart(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void restartCluster() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stopCluster() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void killCluster() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String syncFilesFromServer(int server, String dir) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String syncFilesToServer(int server, String path) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ComponentStatus getStatus() {
      return fromDriver.getStatus();
   }

   @Override
   public InfinispanServerTestConfiguration getConfiguration() {
      // We only use the config from the fromDriver.. this should be okay
      return fromDriver.getConfiguration();
   }

   protected ContainerInfinispanServerDriver driverToUse(int server) {
      if (fromDriver.isRunning(server)) {
         return fromDriver;
      }
      return toDriver;
   }

   protected int offsetToUse(ContainerInfinispanServerDriver driverUsed, int server) {
      if (driverUsed == fromDriver) {
         return server;
      }
      // We have to invert the position as our to will only have up to the number of not running from servers
      return fromDriver.serverCount() - 1 - server;
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server, String username, String password, Consumer<Closeable> reaper) {
      ContainerInfinispanServerDriver driver = driverToUse(server);
      return driver.getJmxConnection(offsetToUse(driver, server), username, password, reaper);
   }

   @Override
   public RemoteCacheManager createRemoteCacheManager(ConfigurationBuilder builder) {
      return new RemoteCacheManager(builder.build());
   }

   @Override
   public int getTimeout() {
      return fromDriver.getTimeout();
   }

   @Override
   public boolean isRunning(int server) {
      return fromDriver.serverCount() >= server;
   }

   @Override
   public int serverCount() {
      return fromDriver.serverCount();
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      ContainerInfinispanServerDriver driver = driverToUse(server);
      return driver.getServerSocket(offsetToUse(driver, server), port);
   }

   @Override
   public InetAddress getServerAddress(int server) {
      ContainerInfinispanServerDriver driver = driverToUse(server);
      return driver.getServerAddress(offsetToUse(driver, server));
   }
}
