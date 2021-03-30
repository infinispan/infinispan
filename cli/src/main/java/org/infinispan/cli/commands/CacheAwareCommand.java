package org.infinispan.cli.commands;

import java.util.Optional;

import org.infinispan.cli.resources.Resource;

/**
 * An interface for CLI commands to implement which require a cache name.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public interface CacheAwareCommand {

   Optional<String> getCacheName(Resource activeResource);

}
