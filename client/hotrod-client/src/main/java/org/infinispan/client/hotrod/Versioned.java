package org.infinispan.client.hotrod;
/**
 * Versioned
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface Versioned {
   long getVersion();
}
