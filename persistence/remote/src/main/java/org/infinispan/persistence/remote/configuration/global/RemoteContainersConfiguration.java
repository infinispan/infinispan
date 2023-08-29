package org.infinispan.persistence.remote.configuration.global;

import java.util.Map;

import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * @since 15.0
 **/
@BuiltBy(RemoteContainersConfigurationBuilder.class)
@SerializedWith(RemoteContainersConfigurationSerializer.class)
public class RemoteContainersConfiguration {
   private final Map<String, RemoteContainerConfiguration> containers;
   private final TransportFactory transportFactory;

   public RemoteContainersConfiguration(Map<String, RemoteContainerConfiguration> containers, TransportFactory transportFactory) {
      this.containers = containers;
      this.transportFactory = transportFactory;
   }

   public Map<String, RemoteContainerConfiguration> configurations() {
      return containers;
   }

   public TransportFactory transportFactory() {
      return transportFactory;
   }
}
