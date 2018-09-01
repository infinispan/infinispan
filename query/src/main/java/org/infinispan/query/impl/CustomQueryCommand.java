package org.infinispan.query.impl;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Custom commands from the Query module should implement this interface to fetch needed components.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public interface CustomQueryCommand {

   void setCacheManager(EmbeddedCacheManager cm);
}
