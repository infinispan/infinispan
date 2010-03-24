package hotrod;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public enum Flag {
   ZERO_LOCK_ACQUISITION_TIMEOUT(0x0001),
   CACHE_MODE_LOCAL(0x0002),
   SKIP_LOCKING(0x0004),
   FORCE_WRITE_LOCK(0x0008),
   SKIP_CACHE_STATUS_CHECK(0x0010),
   FORCE_ASYNCHRONOUS(0x0020),
   FORCE_SYNCHRONOUS(0x0040),
   SKIP_CACHE_STORE(0x0100),
   FAIL_SILENTLY(0x0200),
   SKIP_REMOTE_LOOKUP(0x0400),
   PUT_FOR_EXTERNAL_READ(0x0800);

   private int flagInt;

   Flag(int flagInt) {
      this.flagInt = flagInt;
   }

   public int getFlagInt() {
      return flagInt;
   }
}
