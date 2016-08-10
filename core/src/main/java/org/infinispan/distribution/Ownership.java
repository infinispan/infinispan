package org.infinispan.distribution;

public enum Ownership {
   /**
    * This node is not an owner.
    */
   NON_OWNER,
   /**
    * This node is the primary owner.
    */
   PRIMARY,
   /**
    * this node is the backup owner.
    */
   BACKUP
}
