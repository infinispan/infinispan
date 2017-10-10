package org.infinispan.lock.impl.externalizers;

/**
 * Ids range: 2100 - 2149
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public interface ExternalizerIds {

   Integer CLUSTERED_LOCK_KEY = 2100;
   Integer CLUSTERED_LOCK_VALUE = 2101;
   Integer LOCK_FUNCTION = 2102;
   Integer UNLOCK_FUNCTION = 2103;
   Integer IS_LOCKED_FUNCTION = 2104;
   Integer CLUSTERED_LOCK_FILTER = 2105;
}
