package org.infinispan.rest.operations.mime;

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * Build for mime metadata
 *
 * @author wburns
 * @since 9.0
 */
public class MimeMetadataBuilder extends EmbeddedMetadata.Builder {

   private String contentType;

   public MimeMetadataBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
   }

   @Override
   public Metadata build() {
      return new MimeExpirableMetadata(contentType, records, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }
}
