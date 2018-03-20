package org.infinispan.lock.api;

import org.infinispan.commons.util.Experimental;

/**
 * A Clustered Lock can be reentrant and there are different ownership levels.
 * <p>
 * The only mode supported now is "non reentrant" locks for "nodes".
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
@Experimental
public class ClusteredLockConfiguration {
   private final OwnershipLevel ownershipLevel; // default NODE
   private final boolean reentrant; // default false

   /**
    * Default lock is non reentrant and the ownership level is {@link OwnershipLevel#NODE}
    */
   public ClusteredLockConfiguration() {
      this.ownershipLevel = OwnershipLevel.NODE;
      this.reentrant = false;
   }

   /**
    * @return true if the lock is reentrant
    */
   public boolean isReentrant() {
      return reentrant;
   }

   /**
    * @return the {@link OwnershipLevel} or this lock
    */
   public OwnershipLevel getOwnershipLevel() {
      return ownershipLevel;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusteredLockConfiguration{");
      sb.append("ownershipLevel=").append(ownershipLevel.name());
      sb.append(", reentrant=").append(reentrant);
      sb.append('}');
      return sb.toString();
   }
}
