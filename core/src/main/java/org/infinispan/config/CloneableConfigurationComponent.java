package org.infinispan.config;

import java.io.Serializable;

/**
 * Interface for all configurable components
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface CloneableConfigurationComponent extends Serializable, Cloneable {
   CloneableConfigurationComponent clone() throws CloneNotSupportedException;
}
