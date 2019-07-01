package org.infinispan.server.test.security.rest;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_C;

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
    protected RESTHelper rest;

    protected void securedReadWriteOperations() throws Exception {
        rest.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        rest.put(rest.fullPathKey(KEY_A), "data", "text/plain", HttpStatus.SC_OK);
        rest.clearCredentials();
        rest.put(rest.fullPathKey(KEY_B), "data", "text/plain", HttpStatus.SC_UNAUTHORIZED);
        rest.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        rest.post(rest.fullPathKey(KEY_C), "data", "text/plain", HttpStatus.SC_OK);
        rest.clearCredentials();
        rest.post(rest.fullPathKey(KEY_D), "data", "text/plain", HttpStatus.SC_UNAUTHORIZED);
        rest.get(rest.fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        rest.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        rest.get(rest.fullPathKey(KEY_A), "data");
        rest.clearCredentials();
        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        rest.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_OK);
        rest.clearCredentials();
        rest.delete(rest.fullPathKey(KEY_A), HttpStatus.SC_UNAUTHORIZED);
        rest.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
        rest.delete(rest.fullPathKey(KEY_A), HttpStatus.SC_OK);
        rest.delete(rest.fullPathKey(KEY_C), HttpStatus.SC_OK);
        rest.clearCredentials();
    }

    protected void authzOperations() throws Exception {
        rest.setCredentials(WRITER_LOGIN, WRITER_PASSWD);
        rest.put(rest.fullPathKey(AUTHZ_CACHE, KEY_A), "data", "text/plain", HttpStatus.SC_OK);
        rest.get(rest.fullPathKey(AUTHZ_CACHE, KEY_A), HttpStatus.SC_FORBIDDEN);
        rest.setCredentials(READER_LOGIN, READER_PASSWD);
        rest.put(rest.fullPathKey(AUTHZ_CACHE, KEY_A), "data", "text/plain", HttpStatus.SC_FORBIDDEN);
        rest.get(rest.fullPathKey(AUTHZ_CACHE, KEY_A), "data");
        rest.delete(rest.fullPathKey(AUTHZ_CACHE, KEY_A), HttpStatus.SC_FORBIDDEN);
        rest.setCredentials(WRITER_LOGIN, WRITER_PASSWD);
        rest.delete(rest.fullPathKey(AUTHZ_CACHE, KEY_A), HttpStatus.SC_OK);
    }

    protected void schemaCacheAccess() throws Exception {
        String proto = Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader());
        rest.setCredentials(WRITER_LOGIN, WRITER_PASSWD);
        rest.put(rest.fullPathKey(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME, "bank.proto"), proto, "text/plain", HttpStatus.SC_FORBIDDEN);
        rest.setCredentials(ADMIN_LOGIN, ADMIN_PASSWD);
        rest.put(rest.fullPathKey(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME, "bank.proto"), proto, "text/plain", HttpStatus.SC_OK);
    }
}
