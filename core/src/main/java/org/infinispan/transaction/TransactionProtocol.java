package org.infinispan.transaction;

/**
 * Date: 1/13/12
 * Time: 4:05 PM
 *
 * @author pruivo
 */
public enum TransactionProtocol {
    NORMAL,
    TOTAL_ORDER;

    public boolean isTotalOrder() {
        return this == TOTAL_ORDER;
    }
}
