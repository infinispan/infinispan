package org.infinispan.persistence.remote.configuration.global;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * @since 15.0
 **/
public class RemoteContainersConfigurationBuilder implements Builder<RemoteContainersConfiguration> {
   private final GlobalConfigurationBuilder global;
   private final Map<String, RemoteContainerConfigurationBuilder> builders = new HashMap<>();
   private TransportFactory transportFactory = TransportFactory.DEFAULT;

   public RemoteContainersConfigurationBuilder(GlobalConfigurationBuilder global) {
      this.global = global;
   }

   public RemoteContainerConfigurationBuilder addRemoteContainer(String name) {
      return builders.computeIfAbsent(name, __ -> new RemoteContainerConfigurationBuilder(global));
   }

   public RemoteContainersConfigurationBuilder transportFactory(TransportFactory transportFactory) {
      this.transportFactory = transportFactory;
      return this;
   }

   @Override
   public RemoteContainersConfiguration create() {
      Map<String, RemoteContainerConfiguration> containers = builders.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().create()));
      return new RemoteContainersConfiguration(containers, transportFactory);
   }

   @Override
   public Builder<?> read(RemoteContainersConfiguration template, Combine combine) {

      return null;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

}
