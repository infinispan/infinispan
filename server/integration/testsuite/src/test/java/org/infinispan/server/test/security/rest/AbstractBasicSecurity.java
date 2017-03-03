package org.infinispan.server.test.security.rest;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_C;
import static org.infinispan.server.test.client.rest.RESTHelper.delete;
import static org.infinispan.server.test.client.rest.RESTHelper.fullPathKey;
import static org.infinispan.server.test.client.rest.RESTHelper.get;
import static org.infinispan.server.test.client.rest.RESTHelper.head;
import static org.infinispan.server.test.client.rest.RESTHelper.post;
import static org.infinispan.server.test.client.rest.RESTHelper.put;

import org.apache.http.HttpStatus;
import org.infinispan.server.test.client.rest.RESTHelper;

/**
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 7.0
 *
 */
public abstract class AbstractBasicSecurity {
    private static final String TEST_USER_NAME = "testuser";
    //password encoded as is stored in application-users.properties on the server
    private static final String TEST_USER_PASSWORD = "testpassword";
    private static final String KEY_D = "d";

    protected void securedReadWriteOperations() throws Exception {
        RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        put(fullPathKey(KEY_A), "data", "application/text", HttpStatus.SC_OK);
        RESTHelper.clearCredentials();
        put(fullPathKey(KEY_B), "data", "application/text", HttpStatus.SC_UNAUTHORIZED);
        RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        post(fullPathKey(KEY_C), "data", "application/text", HttpStatus.SC_OK);
        RESTHelper.clearCredentials();
        post(fullPathKey(KEY_D), "data", "application/text", HttpStatus.SC_UNAUTHORIZED);
        get(fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        get(fullPathKey(KEY_A), "data");
        RESTHelper.clearCredentials();
        head(fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        head(fullPathKey(KEY_A), HttpStatus.SC_OK);
        RESTHelper.clearCredentials();
        delete(fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        delete(fullPathKey(KEY_A), HttpStatus.SC_OK);
        delete(fullPathKey(KEY_C), HttpStatus.SC_OK);
        RESTHelper.clearCredentials();
    }
}
