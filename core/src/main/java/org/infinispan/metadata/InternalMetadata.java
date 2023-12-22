package org.infinispan.metadata;

/**
 * @author Mircea Markus
 * @since 6.0
 * @deprecated since 10.0
 */
@Deprecated(forRemoval=true, since = "10.0")
public interface InternalMetadata extends Metadata {

   long created();

   long lastUsed();

   boolean isExpired(long now);

   long expiryTime();
}
