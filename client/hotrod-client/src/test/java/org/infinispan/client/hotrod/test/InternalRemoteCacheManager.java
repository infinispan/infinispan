package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.Marshaller;

import java.net.URL;
import java.util.Properties;

/**
 * RemoteCacheManager that exposes internal components such as transportFactory.
 *
 * This class serves testing purposes and is NOT part of public API.
 *
 * @author Martin Gencur
 */
public class InternalRemoteCacheManager extends RemoteCacheManager {

   public InternalRemoteCacheManager(Configuration configuration) {
      super(configuration, true);
   }

   public InternalRemoteCacheManager(Configuration configuration, boolean start) {
      super(configuration, start);
   }

   public InternalRemoteCacheManager(boolean start) {
      super(start);
   }

   public InternalRemoteCacheManager() {
      this(true);
   }

   @Deprecated
   public InternalRemoteCacheManager(Marshaller marshaller, Properties props, boolean start) {
      super(marshaller, props, start);
   }

   @Deprecated
   public InternalRemoteCacheManager(Marshaller marshaller, Properties props, boolean start, ClassLoader classLoader, ExecutorFactory asyncExecutorFactory) {
      super(marshaller, props, start, classLoader, asyncExecutorFactory);
   }

   @Deprecated
   public InternalRemoteCacheManager(Marshaller marshaller, Properties props) {
      this(marshaller, props, true);
   }

   @Deprecated
   public InternalRemoteCacheManager(Marshaller marshaller, Properties props, ExecutorFactory asyncExecutorFactory) {
      super(marshaller, props, asyncExecutorFactory);
   }

   @Deprecated
   public InternalRemoteCacheManager(Marshaller marshaller, Properties props, ClassLoader classLoader) {
      super(marshaller, props, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(Properties props, boolean start) {
      super(props, start);
   }

   @Deprecated
   public InternalRemoteCacheManager(Properties props, boolean start, ClassLoader classLoader, ExecutorFactory asyncExecutorFactory) {
      super(props, start, classLoader, asyncExecutorFactory);
   }

   @Deprecated
   public InternalRemoteCacheManager(Properties props) {
      super(props);
   }

   @Deprecated
   public InternalRemoteCacheManager(Properties props, ClassLoader classLoader) {
      super(props, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(String host, int port, boolean start) {
      super(host, port, start);
   }

   @Deprecated
   public InternalRemoteCacheManager(String host, int port, boolean start, ClassLoader classLoader) {
      super(host, port, start, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(String host, int port) {
      super(host, port);
   }

   @Deprecated
   public InternalRemoteCacheManager(String host, int port, ClassLoader classLoader) {
      this(host, port, true, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(String servers, boolean start) {
      super(servers, start);
   }

   @Deprecated
   public InternalRemoteCacheManager(String servers, boolean start, ClassLoader classLoader) {
      super(servers, start, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(String servers) {
      super(servers);
   }

   @Deprecated
   public InternalRemoteCacheManager(String servers, ClassLoader classLoader) {
      this(servers, true, classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(URL config, boolean start) {
      super(config, start);
   }

   @Deprecated
   public InternalRemoteCacheManager(URL config, boolean start, ClassLoader classLoader) {
      super(config,start,classLoader);
   }

   @Deprecated
   public InternalRemoteCacheManager(URL config) {
      super(config);
   }

   @Deprecated
   public InternalRemoteCacheManager(URL config, ClassLoader classLoader) {
      this(config, true, classLoader);
   }

   public TransportFactory getTransportFactory() {
      return transportFactory;
   }
}
