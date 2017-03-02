package org.infinispan.manager;

import org.infinispan.commons.CacheException;

/**
 * Exception thrown when a configuration was defined, but one already existed for that name
 * @author wburns
 * @since 9.0
 */
public class ConfigurationAlreadyDefinedException extends CacheException {

}
