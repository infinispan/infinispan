/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.jndi;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.Hashtable;

public class DummyContextFactory implements InitialContextFactory {
   static Context instance = new DummyContext();

   /**
    * Creates an Initial Context for beginning name resolution. Special requirements of this context are supplied using
    * <code>environment</code>.
    * <p/>
    * The environment parameter is owned by the caller. The implementation will not modify the object or keep a
    * reference to it, although it may keep a reference to a clone or copy.
    *
    * @param environment The possibly null environment specifying information to be used in the creation of the initial
    *                    context.
    * @return A non-null initial context object that implements the Context interface.
    * @throws javax.naming.NamingException If cannot create an initial context.
    */
   public Context getInitialContext(Hashtable environment) throws NamingException {
      return instance;
   }
}
