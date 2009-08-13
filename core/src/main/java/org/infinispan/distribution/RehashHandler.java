package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface RehashHandler {

   void rehash(DataContainer dc, CacheStore cacheStore, RpcManager rpc, CommandsFactory cf);

}
