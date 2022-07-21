package org.infinispan.hotrod.api;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.api.async.BackpressureStrategy;
import org.infinispan.api.common.Flags;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;

public interface ClientCacheListenerOptions extends CacheListenerOptions {
   CacheListenerOptions DEFAULT = new Impl();

   Optional<String> filterFactoryName();

   Optional<byte[][]> filterFactoryParameters();

   Optional<String> converterFactoryName();

   Optional<byte[][]> converterFactoryParameters();

   boolean useRawData();

   class Impl extends CacheListenerOptions.Impl implements ClientCacheListenerOptions {
      private final String filterFactoryName;
      private final byte[][] filterFactoryParams;
      private final String converterFactoryName;
      private final byte[][] converterFactoryParams;
      private final boolean useRawData;

      private Impl() {
         this(null, null, false, null, null, null, null, false, clientCacheListenerOptions(),
               BackpressureStrategy.BUFFER);
      }

      public Impl(Duration timeout, Flags<?, ?> flags, boolean includeCurrentState, String filterFactoryName,
            byte[][] filterFactoryParams, String converterFactoryName, byte[][] converterFactoryParams,
            boolean useRawData, Set<CacheEntryEventType> eventTypes, BackpressureStrategy backpressureStrategy) {
         super(timeout, flags, includeCurrentState, eventTypes, backpressureStrategy);

         this.filterFactoryName = filterFactoryName;
         this.filterFactoryParams = filterFactoryParams;
         this.converterFactoryName = converterFactoryName;
         this.converterFactoryParams = converterFactoryParams;
         this.useRawData = useRawData;
      }

      public static ClientCacheListenerOptions.Impl fromCacheListenerOperations(CacheListenerOptions options) {
         if (options instanceof ClientCacheListenerOptions.Impl) {
            return (ClientCacheListenerOptions.Impl) options;
         }
         if (options instanceof ClientCacheListenerOptions) {
            ClientCacheListenerOptions clientOptions = (ClientCacheListenerOptions) options;
            return new ClientCacheListenerOptions.Impl(options.timeout().orElse(null), options.flags().orElse(null),
                  options.includeCurrentState(), clientOptions.filterFactoryName().orElse(null),
                  clientOptions.filterFactoryParameters().orElse(null),
                  clientOptions.converterFactoryName().orElse(null),
                  clientOptions.converterFactoryParameters().orElse(null), clientOptions.useRawData(),
                  clientOptions.eventTypes(), clientOptions.backpressureStrategy());
         }
         if (options instanceof CacheListenerOptions.Impl) {
            CacheListenerOptions.Impl impl = (CacheListenerOptions.Impl) options;
            return new ClientCacheListenerOptions.Impl(impl.rawTimeout(), impl.rawFlags(), options.includeCurrentState(),
                  null, null, null, null, false, clientCacheListenerOptions(), impl.backpressureStrategy());
         }
         return new ClientCacheListenerOptions.Impl(options.timeout().orElse(null), options.flags().orElse(null),
               options.includeCurrentState(), null, null, null, null, false, clientCacheListenerOptions(), options.backpressureStrategy());
      }

      @Override
      public Optional<String> filterFactoryName() {
         return Optional.of(filterFactoryName);
      }

      public String rawFilterFactoryName() {
         return filterFactoryName;
      }

      @Override
      public Optional<byte[][]> filterFactoryParameters() {
         return Optional.of(filterFactoryParams);
      }

      public byte[][] rawFilterFactoryParameters() {
         return filterFactoryParams;
      }

      @Override
      public Optional<String> converterFactoryName() {
         return Optional.of(converterFactoryName);
      }

      public String rawConverterFactoryName() {
         return converterFactoryName;
      }

      @Override
      public Optional<byte[][]> converterFactoryParameters() {
         return Optional.of(converterFactoryParams);
      }

      public byte[][] rawConverterFactoryParameters() {
         return converterFactoryParams;
      }

      @Override
      public boolean useRawData() {
         return useRawData;
      }
   }

   static Set<CacheEntryEventType> clientCacheListenerOptions() {
      Set<CacheEntryEventType> eventTypes = new HashSet<>(CacheListenerOptions.commonCacheEventTypes());
      eventTypes.addAll(Arrays.asList(ClientCacheEventTypes.values()));
      return eventTypes;
   }

   class Builder extends CacheListenerOptions.Builder {
      String filterFactoryName;
      byte[][] filterFactoryParams;
      String converterFactoryName;
      byte[][] convererFactoryParams;
      boolean useRawData;

      @Override
      public ClientCacheListenerOptions build() {
         if (eventTypes == null) {
            eventTypes = clientCacheListenerOptions();
         }
         return new Impl(timeout, flags, includeCurrentState, filterFactoryName, filterFactoryParams,
               converterFactoryName, convererFactoryParams, useRawData, eventTypes, backpressureStrategy);
      }

      @Override
      public Builder includeCurrentState(boolean includeCurrentState) {
         super.includeCurrentState(includeCurrentState);
         return this;
      }

      @Override
      public Builder timeout(Duration timeout) {
         super.timeout(timeout);
         return this;
      }

      @Override
      public Builder flags(Flags<?, ?> flags) {
         super.flags(flags);
         return this;
      }

      @Override
      public Builder backpressureStrategy(BackpressureStrategy backpressureStrategy) {
         super.backpressureStrategy(backpressureStrategy);
         return this;
      }

      public Builder filterFactoryName(String filterFactoryName) {
         this.filterFactoryName = filterFactoryName;
         return this;
      }

      public Builder filterFactoryParameters(byte[][] filterFactoryParams) {
         this.filterFactoryParams = filterFactoryParams;
         return this;
      }

      public Builder converterFactoryName(String converterFactoryName) {
         this.converterFactoryName = converterFactoryName;
         return this;
      }

      public Builder converterFactoryParameters(byte[][] converterFactoryParams) {
         this.convererFactoryParams = converterFactoryParams;
         return this;
      }

      public Builder useRawData() {
         return useRawData(true);
      }

      public Builder useRawData(boolean useRawData) {
         this.useRawData = useRawData;
         return this;
      }
   }
}
