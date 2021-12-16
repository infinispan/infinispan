package org.infinispan.xsite.irac;

import org.infinispan.commands.CommandInvocationId;

/**
 * Basic information about a key stored in {@link IracManager}.
 * <p>
 * It includes the segment the owner of the key. The owner, in the {@link  IracManager} context, is the {@link
 * CommandInvocationId} or transaction which updated (or removed) the key.
 *
 * @since 14
 */
public interface IracManagerKeyInfo {

   /**
    * @return The key.
    */
   Object getKey();

   /**
    * @return The owner who updated the key.
    */
   Object getOwner();

   /**
    * @return The key's segment.
    */
   int getSegment();

}
