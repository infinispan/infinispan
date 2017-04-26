package org.infinispan.server.core;

import org.infinispan.metadata.Metadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface ServerMetadata extends Metadata {

   long streamVersion();
}
