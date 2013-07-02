package org.infinispan.query;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public interface ProjectionConstants {

   String ID = org.hibernate.search.ProjectionConstants.ID;

   String KEY = "__ISPN_Key";

   String VALUE = org.hibernate.search.ProjectionConstants.THIS;

}
