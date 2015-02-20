package org.infinispan.test.integration.as.wildfly.util;

import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.Search;

import javax.enterprise.inject.Produces;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * This class uses CDI to alias Java EE resources, such as the persistence context, to CDI beans
 * <p/>
 * <p> Example injection on a managed bean field: </p>
 * <p/>
 * <pre>
 * &#064;Inject
 * private EntityManager em;
 * </pre>
 */
public class Resources {

   @Produces
   @PersistenceContext
   private EntityManager em;

   @SuppressWarnings("unused")
   @Produces
   private FullTextEntityManager getFullTextEntityManager() {
      return Search.getFullTextEntityManager(em);
   }
}
