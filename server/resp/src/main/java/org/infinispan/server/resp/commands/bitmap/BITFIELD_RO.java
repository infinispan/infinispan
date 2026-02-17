package org.infinispan.server.resp.commands.bitmap;

import org.infinispan.server.resp.AclCategory;

/**
 * Executes the Redis BITFIELD_RO command.
 * The BITFIELD command is a versatile tool for manipulating bitfields stored in Redis strings.
 * It allows for setting, getting, and incrementing arbitrary-width signed and unsigned integers
 * at any position within the string.
 * This implementation supports the GET, SET, and INCRBY subcommands, along with the OVERFLOW
 * option for fine-grained control over increment and set operations that exceed the specified
 * integer size.
 *
 * @see <a href="https://redis.io/commands/bitfield_ro/">Redis BITFIELD_RO command</a>
 * @since 16.2
 */
public class BITFIELD_RO extends BITFIELD {

   public BITFIELD_RO() {
      super(true, AclCategory.BITMAP.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }
}
