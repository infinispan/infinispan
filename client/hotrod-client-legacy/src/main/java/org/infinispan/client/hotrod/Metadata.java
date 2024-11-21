package org.infinispan.client.hotrod;

/**
 * Represents metadata about an entry, such as creation and access times and expiration information. Time values are server
 * time representations as returned by {@link org.infinispan.commons.time.TimeService#wallClockTime}
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface Metadata {
   /**
    *
    * @return Time when entry was created. -1 for immortal entries.
    */
   long getCreated();

   /**
    *
    * @return Lifespan of the entry in seconds. Negative values are interpreted as unlimited
    *         lifespan.
    */
   int getLifespan();

   /**
    *
    * @return Time when entry was last used. -1 for immortal entries.
    */
   long getLastUsed();

   /**
    *
    * @return The maximum amount of time (in seconds) this key is allowed to be idle for before it
    *         is considered as expired.
    */
   int getMaxIdle();
}
