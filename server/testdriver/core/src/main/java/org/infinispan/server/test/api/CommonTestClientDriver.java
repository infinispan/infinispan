package org.infinispan.server.test.api;

import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.security.AuthorizationPermission;

public interface CommonTestClientDriver<T extends CommonTestClientDriver<T>> {
   T withServerConfiguration(org.infinispan.configuration.cache.ConfigurationBuilder serverConfiguration);

   T withServerConfiguration(StringConfiguration configuration);

   T withCacheMode(CacheMode mode);

   T withUser(TestUser testUser);

   T withUser(AuthorizationPermission permission);

   T withQualifiers(Object... qualifiers);

   default T withQualifier(String qualifier) {
      return withQualifiers(qualifier);
   }
}
