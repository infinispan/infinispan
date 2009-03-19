package org.horizon.invocation;

/**
 * Available options, which may be set on a per-invocation basis
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum Options {
   ZERO_LOCK_ACQUISITION_TIMEOUT,
   CACHE_MODE_LOCAL,
   SKIP_LOCKING,
   FORCE_WRITE_LOCK,
   SKIP_CACHE_STATUS_CHECK,
   FORCE_ASYNCHRONOUS,
   FORCE_SYNCHRONOUS,
   SKIP_CACHE_STORE,
   FAIL_SILENTLY

}
