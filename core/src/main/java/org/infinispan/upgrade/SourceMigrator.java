package org.infinispan.upgrade;

/**
 * Performs migration operations on the source server or cluster of servers
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface SourceMigrator {
   /**
    * Records all known keys and stores them under a well-known key which can be used for retrieval.
    */
   void recordKnownGlobalKeyset();

   String getCacheName();
}
