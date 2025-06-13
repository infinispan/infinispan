package org.infinispan.server.test.api;

import java.util.EnumSet;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;

/**
 * Base class for the driver API
 *
 * @since 10
 * @param <S>
 * @author Tristan Tarrant
 */
abstract class AbstractTestClientDriver<S extends AbstractTestClientDriver<S>> implements Self<S>, CommonTestClientDriver<S> {
   protected BasicConfiguration serverConfiguration = null;
   protected EnumSet<CacheContainerAdmin.AdminFlag> flags = EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class);
   protected CacheMode mode = null;
   protected Object[] qualifiers;

   static StringConfiguration forCacheMode(CacheMode mode) {
      return switch (mode) {
         case LOCAL -> DefaultTemplate.LOCAL.getConfiguration();
         case DIST_ASYNC -> DefaultTemplate.DIST_ASYNC.getConfiguration();
         case DIST_SYNC -> DefaultTemplate.DIST_SYNC.getConfiguration();
         case REPL_ASYNC -> DefaultTemplate.REPL_ASYNC.getConfiguration();
         case REPL_SYNC -> DefaultTemplate.REPL_SYNC.getConfiguration();
         default -> throw new IllegalArgumentException(mode.toString());
      };
   }

   @Override
   public S withServerConfiguration(org.infinispan.configuration.cache.ConfigurationBuilder serverConfiguration) {
      if (mode != null) {
         throw new IllegalStateException("Cannot set server configuration and cache mode");
      }
      this.serverConfiguration = serverConfiguration.build();
      return self();
   }

   @Override
   public S withServerConfiguration(StringConfiguration configuration) {
      if (mode != null) {
         throw new IllegalStateException("Cannot set server configuration and cache mode");
      }
      this.serverConfiguration = configuration;
      return self();
   }

   @Override
   public S withCacheMode(CacheMode mode) {
      if (serverConfiguration != null) {
         throw new IllegalStateException("Cannot set server configuration and cache mode");
      }
      this.mode = mode;
      return self();
   }

   public S withQualifier(String qualifier) {
      return withQualifiers(qualifier);
   }

   public S withQualifiers(Object... qualifiers) {
      this.qualifiers = qualifiers;
      return self();
   }

   public S makeVolatile() {
      this.flags = EnumSet.of(CacheContainerAdmin.AdminFlag.VOLATILE);
      return self();
   }

}
