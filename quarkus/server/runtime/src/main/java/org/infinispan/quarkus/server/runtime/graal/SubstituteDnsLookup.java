package org.infinispan.quarkus.server.runtime.graal;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.infinispan.server.context.ServerInitialContextFactory;
import org.infinispan.server.context.ServerInitialContextFactoryBuilder;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteDnsLookup {
}

@TargetClass(ServerInitialContextFactoryBuilder.class)
final class Target_ServerInitialContextFactoryBuilder {
   @Alias
   private ConcurrentMap<String, Object> namedObjects;

   @Substitute
   public InitialContextFactory createInitialContextFactory(Hashtable<?, ?> environment) throws NamingException {
      String className = environment != null ? (String) environment.get(Context.INITIAL_CONTEXT_FACTORY) : null;
      if (className == null) {
         return new ServerInitialContextFactory(namedObjects);
      }
      switch (className) {
         case "com.sun.jndi.dns.DnsContextFactory":
            return new com.sun.jndi.dns.DnsContextFactory();
         case "com.sun.jndi.ldap.LdapCtxFactory":
            return new com.sun.jndi.ldap.LdapCtxFactory();
         default:
            throw new NamingException("Native Infinispan Server does not support " + className + " as an InitialContextFactory");
      }
   }
}
