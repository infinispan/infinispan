package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
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

   public TransportFactory getTransportFactory() {
      return transportFactory;
   }
}
