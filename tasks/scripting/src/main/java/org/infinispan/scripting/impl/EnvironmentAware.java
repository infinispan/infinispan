package org.infinispan.scripting.impl;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * EnvironmentAware.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface EnvironmentAware {
   void setEnvironment(EmbeddedCacheManager cacheManager, Marshaller marshaller);
}
