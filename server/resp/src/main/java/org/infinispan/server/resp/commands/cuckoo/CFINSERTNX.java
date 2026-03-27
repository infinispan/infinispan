package org.infinispan.server.resp.commands.cuckoo;

/**
 * CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
 * <p>
 * Adds one or more items to a Cuckoo filter only if they don't already exist.
 *
 * @see <a href="https://redis.io/commands/cf.insertnx/">CF.INSERTNX</a>
 * @since 16.2
 */
public class CFINSERTNX extends CFINSERT {

   public CFINSERTNX() {
      super("CF.INSERTNX");
   }

   @Override
   protected boolean isOnlyIfNotExists() {
      return true;
   }
}
