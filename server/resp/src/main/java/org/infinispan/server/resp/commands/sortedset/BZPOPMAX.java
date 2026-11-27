package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.commands.sortedset.internal.BZPOP;

/**
 * BZPOPMAX key [key ...] timeout
 * <p>
 * Blocking variant of {@link ZPOPMAX}: pops the member with the highest score from the first non-empty sorted set,
 * blocking until a member is available or the timeout elapses.
 *
 * @see <a href="https://redis.io/commands/bzpopmax/">BZPOPMAX</a>
 * @since 16.4
 */
public class BZPOPMAX extends BZPOP {

   public BZPOPMAX() {
      super(false, AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.FAST.mask() | AclCategory.BLOCKING.mask());
   }
}
