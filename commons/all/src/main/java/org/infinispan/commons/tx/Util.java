package org.infinispan.commons.tx;

import javax.transaction.Status;

/**
 * Transaction related util class.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class Util {

   private Util() {
   }

   public static String transactionStatusToString(int status) {
      switch (status) {
         case Status.STATUS_ACTIVE:
            return "ACTIVE";
         case Status.STATUS_MARKED_ROLLBACK:
            return "MARKED_ROLLBACK";
         case Status.STATUS_PREPARED:
            return "PREPARED";
         case Status.STATUS_COMMITTED:
            return "COMMITTED";
         case Status.STATUS_ROLLEDBACK:
            return "ROLLED_BACK";
         case Status.STATUS_UNKNOWN:
            return "UNKNOWN";
         case Status.STATUS_NO_TRANSACTION:
            return "NO_TRANSACTION";
         case Status.STATUS_PREPARING:
            return "PREPARING";
         case Status.STATUS_COMMITTING:
            return "COMMITTING";
         case Status.STATUS_ROLLING_BACK:
            return "ROLLING_BACK";
         default:
            return "unknown status (" + status + ")";
      }
   }

}
