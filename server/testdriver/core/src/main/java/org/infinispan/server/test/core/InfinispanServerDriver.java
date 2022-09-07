package org.infinispan.server.test.core;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import javax.management.MBeanServerConnection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public interface InfinispanServerDriver {
   Log log = LogFactory.getLog(InfinispanServerDriver.class);

   ComponentStatus getStatus();

   InfinispanServerTestConfiguration getConfiguration();

   /**
    * Prepares a named server configuration
    * @param name the name of the server configuration
    */
   void prepare(String name);

   /**
    * Starts a prepared server configuration
    * @param name the name of the server configuration
    */
   void start(String name);

   /**
    * Stops a running server configuration
    * @param name the name of the server configuration
    */
   void stop(String name);

   /**
    * Determines whether a specific server is running as part of a server configuration
    * @param server the
    * @return true if the server is running, false otherwise
    */
   boolean isRunning(int server);

   /**
    * Returns an InetSocketAddress for connecting to a specific port on a specific server. The implementation will need
    * to provide a specific mapping (e.g. port offset).
    *
    * @param server the index of the server
    * @param port   the service port
    * @return an unresolved InetSocketAddress pointing to the actual running service
    */
   InetSocketAddress getServerSocket(int server, int port);

   /**
    * Returns an InetAddress that points to a specific server.
    *
    * @param server the index of the server
    * @return an InetAddress pointing to the server's address
    */
   InetAddress getServerAddress(int server);

   File getCertificateFile(String name);

   File getRootDir();

   File getConfDir();

   void applyKeyStore(ConfigurationBuilder builder, String certificateName);

   void applyKeyStore(ConfigurationBuilder builder, String certificateName, String type, String provider);

   void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName);

   void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider);

   void applyTrustStore(ConfigurationBuilder builder, String certificateName);

   void applyTrustStore(ConfigurationBuilder builder, String certificateName, String type, String provider);

   void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName);

   void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider);

   /**
    * Pauses a server. Equivalent to kill -SIGSTOP
    *
    * @param server the index of the server
    */
   void pause(int server);

   /**
    * Resumes a paused server. Equivalent to kill -SIGCONT
    *
    * @param server the index of the server
    */
   void resume(int server);

   /**
    * Gracefully stops a running server
    *
    * @param server the index of the server
    */
   void stop(int server);

   /**
    * Forcefully stops a server. Equivalent to kill -SIGKILL
    *
    * @param server the index of the server
    */
   void kill(int server);

   /**
    * Restarts a previously stopped server.
    *
    * @param server the index of the server
    */
   void restart(int server);

   /**
    * Restarts all of the nodes
    */
   void restartCluster();

   /**
    * Returns a {@link MBeanServerConnection} to the specified server
    *
    * @param server the index of the server
    */
   MBeanServerConnection getJmxConnection(int server);

   RemoteCacheManager createRemoteCacheManager(ConfigurationBuilder builder);

   /**
    * Returns the amount of time in seconds that we should wait for a server start/stop operation. This may vary
    * depending on the type of driver (embedded, container)
    *
    * @return the number of seconds after which a server start/stop is considered to timeout
    */
   int getTimeout();

   /**
    * Synchronizes files from the server to the local filesystem
    * @param server the server
    * @param dir if relative, the path relative to the server root, otherwise an absolute path
    * @return the path where the data has been synced to
    */
   String syncFilesFromServer(int server, String dir);

   String syncFilesToServer(int server, String path);
}
