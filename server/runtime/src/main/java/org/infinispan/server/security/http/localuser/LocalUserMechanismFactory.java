package org.infinispan.server.security.http.localuser;

import static org.infinispan.server.security.http.localuser.LocalUserAuthenticationMechanism.SILENT;

import java.security.Provider;
import java.util.Map;
import java.util.Objects;

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
        Objects.requireNonNull(mechanismName, "Mechanism name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        Objects.requireNonNull(callbackHandler, "Callback handler cannot be null");

        if (LocalUserAuthenticationMechanism.LOCALUSER_NAME.equals(mechanismName)) {
            return new LocalUserAuthenticationMechanism(callbackHandler,
                    Boolean.parseBoolean((String) properties.get(SILENT)));
        }

        return null;
    }

}
