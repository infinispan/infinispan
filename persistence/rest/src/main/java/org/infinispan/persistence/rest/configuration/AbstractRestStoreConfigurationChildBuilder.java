package org.infinispan.persistence.rest.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.rest.metadata.MetadataHelper;

/**
 * AbstractRestStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public abstract class AbstractRestStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements RestStoreConfigurationChildBuilder<S> {
   private final RestStoreConfigurationBuilder builder;
   protected final AttributeSet attributes;

   protected AbstractRestStoreConfigurationChildBuilder(RestStoreConfigurationBuilder builder, AttributeSet attributes) {
      super(builder);
      this.attributes = attributes;
      this.builder = builder;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public RestStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      return builder.key2StringMapper(key2StringMapper);
   }

   @Override
   public RestStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass) {
      return builder.key2StringMapper(klass);
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(String metadataHelper) {
      return builder.metadataHelper(metadataHelper);
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper) {
      return builder.metadataHelper(metadataHelper);
   }

   @Override
   public RestStoreConfigurationBuilder host(String host) {
      return builder.host(host);
   }

   @Override
   public RestStoreConfigurationBuilder port(int port) {
      return builder.port(port);
   }

   @Override
   public RestStoreConfigurationBuilder path(String path) {
      return builder.path(path);
   }

   @Override
   public RestStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath) {
      return builder.appendCacheNameToPath(appendCacheNameToPath);
   }

   @Override
   public RestStoreConfigurationBuilder rawValues(boolean rawValues) {
      return builder.rawValues(rawValues);
   }

   @Override
   public RestStoreConfigurationBuilder maxContentLength(int maxContentLength) {
      return builder.maxContentLength(maxContentLength);
   }
}
