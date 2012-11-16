/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.logging;

import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;

/**
 * Informational CLI messages. These start from 19500 so as not to overlap with the logging messages defined in {@link Log}
 * Messages.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value="Synchronized %d entries using migrator '%s' on cache '%s'", id=19500)
   String synchronizedEntries(long count, String cacheName, String migrator);

   @Message(value="Disconnected '%s' migrator source on cache '%s'", id=19501)
   String disonnectedSource(String migratorName, String cacheNname);

   @Message(value="Dumped keys for cache %s", id=19502)
   String dumpedKeys(String cacheName);
}
