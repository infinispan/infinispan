package org.infinispan.xsite.irac;

/**
 * Global result of a multi key cross-site request.
 */
enum IracBatchSendResult {
   /**
    * Cross-site request applied successfully.
    */
   OK,
   /**
    * Cross-site request failed (example: lock timeout). Retry sending.
    */
   RETRY,
   /**
    * Cross-site request failed (network failure). Back-off for a while and retry.
    */
   BACK_OFF_AND_RETRY
}
