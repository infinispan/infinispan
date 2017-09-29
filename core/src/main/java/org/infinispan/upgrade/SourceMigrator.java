package org.infinispan.upgrade;

/**
 * Performs migration operations on the source server or cluster of servers
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface SourceMigrator {

   String getCacheName();
}
