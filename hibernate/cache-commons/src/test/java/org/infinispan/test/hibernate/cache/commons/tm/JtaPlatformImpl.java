/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.test.hibernate.cache.commons.tm;

import org.hibernate.TransactionException;
import org.hibernate.engine.transaction.internal.jta.JtaStatusHelper;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;

import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

/**
 * @author Steve Ebersole
 */
public class JtaPlatformImpl implements JtaPlatform {
	@Override
	public TransactionManager retrieveTransactionManager(){
		return XaTransactionManagerImpl.getInstance();
	}

	@Override
	public UserTransaction retrieveUserTransaction() {
		throw new TransactionException( "UserTransaction not used in these tests" );
	}

	@Override
	public Object getTransactionIdentifier(Transaction transaction) {
		return transaction;
	}

	@Override
	public boolean canRegisterSynchronization() {
		return JtaStatusHelper.isActive( XaTransactionManagerImpl.getInstance() );
	}

	@Override
	public void registerSynchronization(Synchronization synchronization) {
		try {
			XaTransactionManagerImpl.getInstance().getTransaction().registerSynchronization( synchronization );
		}
		catch (Exception e) {
			throw new TransactionException( "Could not obtain transaction from TM" );
		}
	}

	@Override
	public int getCurrentStatus() throws SystemException {
		return JtaStatusHelper.getStatus( XaTransactionManagerImpl.getInstance() );
	}
}
