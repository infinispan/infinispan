package org.horizon.invocation;

/**
 * Available flags, which may be set on a per-invocation basis.  These are provided using the {@link org.horizon.AdvancedCache}
 * interface, using some of the overloaded methods that allow passing in of a variable number of Flags.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum Flag {
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
