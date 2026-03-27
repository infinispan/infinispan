package org.infinispan.server.security.http.localuser;

import static org.wildfly.common.Assert.checkNotNullParam;

import java.security.Provider;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import org.kohsuke.MetaInfServices;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;

/**
 * A {@link HttpServerAuthenticationMechanismFactory} implementation for the LOCALUSER HTTP authentication mechanism.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@MetaInfServices(value = HttpServerAuthenticationMechanismFactory.class)
public class LocalUserMechanismFactory implements HttpServerAuthenticationMechanismFactory {

    public LocalUserMechanismFactory() {
    }

    public LocalUserMechanismFactory(final Provider provider) {
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> properties) {
        return new String[]{ LocalUserAuthenticationMechanism.LOCALUSER_NAME };
    }

    @Override
    public HttpServerAuthenticationMechanism createAuthenticationMechanism(String mechanismName,
            Map<String, ?> properties, CallbackHandler callbackHandler) throws HttpAuthenticationException {
        checkNotNullParam("mechanismName", mechanismName);
        checkNotNullParam("properties", properties);
        checkNotNullParam("callbackHandler", callbackHandler);

        if (LocalUserAuthenticationMechanism.LOCALUSER_NAME.equals(mechanismName)) {
            String challengePath = (String) properties.get(LocalUserAuthenticationMechanism.LOCAL_USER_CHALLENGE_PATH);
            String defaultUser = (String) properties.get(LocalUserAuthenticationMechanism.DEFAULT_USER);
            return new LocalUserAuthenticationMechanism(callbackHandler, challengePath, defaultUser);
        }

        return null;
    }

}
