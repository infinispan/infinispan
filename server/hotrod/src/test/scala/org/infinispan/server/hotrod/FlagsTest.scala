package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.context.Flag
import org.testng.Assert._

/**
 * Appears that optional parameters in annotations result in compiler errors:
 * https://lampsvn.epfl.ch/trac/scala/ticket/1810
 *
 * Keep an eye on that for @Test and @AfterClass annotations
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.FlagsTest")
class FlagsTest {

   def testSingleFlag {
      flag(1)(1) { flags => flags contains Flag.ZERO_LOCK_ACQUISITION_TIMEOUT }
      flag(1 << 1)(1) { flags => flags contains Flag.CACHE_MODE_LOCAL }
      flag(1 << 2)(1) { flags => flags contains Flag.SKIP_LOCKING }
      flag(1 << 3)(1) { flags => flags contains Flag.FORCE_WRITE_LOCK }
      flag(1 << 4)(1) { flags => flags contains Flag.SKIP_CACHE_STATUS_CHECK }
      flag(1 << 5)(1) { flags => flags contains Flag.FORCE_ASYNCHRONOUS }
      flag(1 << 6)(1) { flags => flags contains Flag.FORCE_SYNCHRONOUS }
      flag(1 << 8)(1) { flags => flags contains Flag.SKIP_CACHE_STORE }
      flag(1 << 9)(1) { flags => flags contains Flag.FAIL_SILENTLY }
      flag(1 << 10)(1) { flags => flags contains Flag.SKIP_REMOTE_LOOKUP }
      flag(1 << 11)(1) { flags => flags contains Flag.PUT_FOR_EXTERNAL_READ }
   }

   def testMultipleFlags {
      flag(3)(2) {
         flags => (flags contains Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) &&
                  (flags contains Flag.CACHE_MODE_LOCAL)
      }
      flag(15)(4) {
         flags => (flags contains Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) &&
                  (flags contains Flag.CACHE_MODE_LOCAL) &&
                  (flags contains Flag.SKIP_LOCKING) &&
                  (flags contains Flag.FORCE_WRITE_LOCK)
      }
      flag(0xF7F)(11) {
         flags => (flags contains Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) &&
                  (flags contains Flag.CACHE_MODE_LOCAL) &&
                  (flags contains Flag.SKIP_LOCKING) &&
                  (flags contains Flag.FORCE_WRITE_LOCK) &&
                  (flags contains Flag.SKIP_CACHE_STATUS_CHECK) &&
                  (flags contains Flag.FORCE_ASYNCHRONOUS) &&
                  (flags contains Flag.FORCE_SYNCHRONOUS) &&
                  (flags contains Flag.SKIP_CACHE_STORE) &&
                  (flags contains Flag.FAIL_SILENTLY) &&
                  (flags contains Flag.SKIP_REMOTE_LOOKUP) &&
                  (flags contains Flag.PUT_FOR_EXTERNAL_READ)
      }
   }

//   private def flag(bitFlags: Int)(size: Int)(p: Set[Flags.Value] => Boolean) {
//      var flags = Flags.extractFlags(bitFlags)
//      assert { flags.size == size }
//      assert { true == p(flags) }
//   }

   private def flag(bitFlags: Int)(size: Int)(p: Set[Flag] => Boolean) {
      val flags = Flags.extractFlags(bitFlags)
      assertEquals(flags.size, size)
      assertTrue(p(flags))
   }

}