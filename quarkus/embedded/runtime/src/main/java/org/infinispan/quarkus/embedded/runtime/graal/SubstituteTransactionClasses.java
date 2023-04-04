package org.infinispan.quarkus.embedded.runtime.graal;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

public class SubstituteTransactionClasses {
}

@TargetClass(JBossStandaloneJTAManagerLookup.class)
@Substitute
final class SubstituteJBossStandaloneJTAManagerLookup implements TransactionManagerLookup {
    private static final Log log = LogFactory.getLog(JBossStandaloneJTAManagerLookup.class);

    @Substitute
    public SubstituteJBossStandaloneJTAManagerLookup() {
    }

    @Substitute
    public void init(GlobalConfiguration globalCfg) {
    }

    @Substitute
    public void init(ClassLoader configuration) {
    }

    @Override
    @Substitute
    public TransactionManager getTransactionManager() {
        TransactionManager tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
        if (log.isInfoEnabled())
            log.retrievingTm(tm);
        return tm;
    }

    @Substitute
    public UserTransaction getUserTransaction() {
        return com.arjuna.ats.jta.UserTransaction.userTransaction();
    }

    @Substitute
    public String toString() {
        return "JBossStandaloneJTAManagerLookup";
    }
}
