package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.BZPOP;

/**
 * BZPOPMIN key [key ...] timeout
 * <p>
 * Blocking variant of {@link ZPOPMIN}: pops the member with the lowest score from the first non-empty sorted set,
 * blocking until a member is available or the timeout elapses.
 *
 * @see <a href="https://redis.io/commands/bzpopmin/">BZPOPMIN</a>
 * @since 16.4
 */
public class BZPOPMIN extends BZPOP {

   public BZPOPMIN() {
      super(true, AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask() | AclCategory.BLOCKING.mask());
   }
}
