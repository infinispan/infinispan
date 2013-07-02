package org.infinispan.client.hotrod;

/**
 * Besides the value, also contains a version and expiration information.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface MetadataValue<V> extends VersionedValue<V> {

   long getCreated();

   int getLifespan();

   long getLastUsed();

   int getMaxIdle();

}
