package org.infinispan.rest.configuration;

import org.infinispan.commons.configuration.BuiltBy;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration {
   private ExtendedHeaders extendedHeaders;

   RestServerConfiguration(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
   }

   public ExtendedHeaders extendedHeaders() {
      return extendedHeaders;
   }

   public void extendedHeaders(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
   }

}
