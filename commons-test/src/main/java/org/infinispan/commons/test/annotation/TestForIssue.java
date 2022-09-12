package org.infinispan.commons.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A documentation annotation for notating what JIRA issue is being tested.
 * <p>
 * Copied from <a href="https://github.com/hibernate/hibernate-orm">Hibernate ORM project</a>.
 *
 * @author Steve Ebersole
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface TestForIssue {

   /**
    * The keys of the JIRA issues tested.
    *
    * @return The JIRA issue keys
    */
   String[] jiraKey();

}
