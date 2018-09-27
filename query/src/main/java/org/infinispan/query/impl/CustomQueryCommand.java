package org.infinispan.query.impl;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Custom commands from the Query module should implement this interface to fetch needed components.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
public interface CustomQueryCommand extends CacheRpcCommand {

   void setCacheManager(EmbeddedCacheManager cm);
}
