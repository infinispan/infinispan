package org.infinispan.commands.read;

import org.infinispan.container.entries.InternalCacheEntry;

@Deprecated//Should not be necessary soon according to ISPN-2177
public interface RemoteFetchingCommand {

   /**
    * @see #getRemotelyFetchedValue()
    */
   public void setRemotelyFetchedValue(InternalCacheEntry remotelyFetchedValue);

   /**
    * If the cache needs to go remotely in order to obtain the value associated to this key, then the remote value
    * is stored in this field.
    * TODO: this method should be able to removed with the refactoring from ISPN-2177
    */
   public InternalCacheEntry getRemotelyFetchedValue();

}
