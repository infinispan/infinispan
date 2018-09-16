package org.infinispan.test.jndi;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

public class DummyContextFactory implements InitialContextFactory {
   static Context instance = new DummyContext();

   /**
    * Creates an Initial Context for beginning name resolution. Special requirements of this context are supplied using
    * <code>environment</code>.
    * <p>
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
