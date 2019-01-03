package org.infinispan.persistence.rest.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;

/**
 * MetadataHelper
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface MetadataHelper {

   String getContentType(MarshalledEntry entry);

   Metadata buildMetadata(String contentType,long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);
}
