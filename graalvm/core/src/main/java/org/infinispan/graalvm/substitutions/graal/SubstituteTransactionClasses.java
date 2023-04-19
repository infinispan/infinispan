package org.infinispan.graalvm.substitutions.graal;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

public class SubstituteTransactionClasses {
}

@TargetClass(JBossStandaloneJTAManagerLookup.class)
@Substitute
final class SubstituteJBossStandaloneJTAManagerLookup implements TransactionManagerLookup {
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
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
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
