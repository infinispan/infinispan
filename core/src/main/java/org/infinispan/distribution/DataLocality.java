package org.infinispan.distribution;

/**
 * Used to determine whether a key is mapped to a local node.  Uncertainty indicates a rehash is in progress and the
 * locality of key in question may be in flux.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.2.1
 */
public enum DataLocality {
   LOCAL(true,false),

   NOT_LOCAL(false,false),

   LOCAL_UNCERTAIN(true,true),

   NOT_LOCAL_UNCERTAIN(false,true);

   private final boolean local, uncertain;

   private DataLocality(boolean local, boolean uncertain) {
      this.local = local;
      this.uncertain = uncertain;
   }

   public boolean isLocal() {
      return local;
   }

   public boolean isUncertain() {
      return uncertain;
   }
}
