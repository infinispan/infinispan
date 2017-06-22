/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.cache.infinispan.tm;
<<<<<<< HEAD
<<<<<<< HEAD
import java.util.Properties;
=======

<<<<<<< HEAD
>>>>>>> HHH-8159 - Apply fixups indicated by analysis tools
import javax.transaction.TransactionManager;
=======
>>>>>>> HHH-8741 - More checkstyle cleanups
import java.util.Properties;
import javax.transaction.TransactionManager;

<<<<<<< HEAD
import org.hibernate.cfg.Settings;
<<<<<<< HEAD
import org.hibernate.transaction.TransactionManagerLookup;
=======

import org.hibernate.cfg.Settings;
import org.hibernate.service.jta.platform.spi.JtaPlatform;
=======
=======
import org.hibernate.boot.spi.SessionFactoryOptions;
>>>>>>> HHH-9762 - Complete deprecation of Settings contract
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
>>>>>>> HHH-7556 - Clean up packages

import javax.transaction.TransactionManager;
import java.util.Properties;
>>>>>>> HHH-5949 - Migrate, complete and integrate TransactionFactory as a service

/**
 * Hibernate transaction manager lookup class for Infinispan, so that
 * Hibernate's transaction manager can be hooked onto Infinispan.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class HibernateTransactionManagerLookup implements org.infinispan.transaction.lookup.TransactionManagerLookup {
	private final JtaPlatform jtaPlatform;

   /**
    * Transaction manager lookup constructor.
    *
    * @param settings for the Hibernate application
    * @param properties for the Hibernate application
    */
	public HibernateTransactionManagerLookup(SessionFactoryOptions settings, Properties properties) {
		this.jtaPlatform = settings != null ? settings.getServiceRegistry().getService( JtaPlatform.class ) : null;
	}

	@Override
	public TransactionManager getTransactionManager() throws Exception {
		return jtaPlatform == null ? null : jtaPlatform.retrieveTransactionManager();
	}

}
