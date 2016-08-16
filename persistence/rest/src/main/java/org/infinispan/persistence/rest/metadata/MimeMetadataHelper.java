package org.infinispan.persistence.rest.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.rest.MimeMetadata;
import org.infinispan.rest.MimeMetadataBuilder;

public class MimeMetadataHelper implements MetadataHelper {

   @Override
   public String getContentType(MarshalledEntry entry) {
      //ugly, to be solved together with ISPN-3460
      InternalMetadataImpl mei = (InternalMetadataImpl) entry.getMetadata();
      MimeMetadata metadata = (MimeMetadata) mei.actual();
      return metadata.contentType();
   }

   @Override
   public Metadata buildMetadata(String contentType, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      MimeMetadataBuilder builder = new MimeMetadataBuilder();
      builder.contentType(contentType).lifespan(lifespan, lifespanUnit).maxIdle(maxIdle, maxIdleUnit);
      return builder.build();
   }

}
