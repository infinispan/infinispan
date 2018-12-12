package org.infinispan.persistence.rest.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

public class EmbeddedMetadataHelper implements MetadataHelper {

   @Override
   public String getContentType(MarshallableEntry entry) {
      return "application/octet-stream";
   }

   @Override
   public Metadata buildMetadata(String contentType, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new EmbeddedMetadata.Builder().lifespan(lifespan, lifespanUnit).maxIdle(maxIdle, maxIdleUnit).build();
   }

}
