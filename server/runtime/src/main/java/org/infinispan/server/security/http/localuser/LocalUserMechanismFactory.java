package org.infinispan.server.security.http.localuser;


import java.nio.file.FileSystems;
import java.security.Provider;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.security.auth.callback.CallbackHandler;

import org.infinispan.server.logging.Log;
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

    private static final Log log = Log.getLog(LocalUserMechanismFactory.class);
    private static final boolean POSIX_SUPPORTED = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private static final ConcurrentHashMap<String, byte[]> pendingChallenges = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> sessionTokens = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "localuser-challenge-cleanup");
        t.setDaemon(true);
        return t;
    });

    public LocalUserMechanismFactory() {
    }

    public LocalUserMechanismFactory(final Provider provider) {
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> properties) {
        if (!POSIX_SUPPORTED) {
            return new String[0];
        }
        return new String[]{ LocalUserAuthenticationMechanism.LOCALUSER_NAME };
    }

    @Override
    public HttpServerAuthenticationMechanism createAuthenticationMechanism(String mechanismName,
            Map<String, ?> properties, CallbackHandler callbackHandler) throws HttpAuthenticationException {
        Objects.requireNonNull(mechanismName, "Mechanism name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        Objects.requireNonNull(callbackHandler, "Callback handler cannot be null");

        if (LocalUserAuthenticationMechanism.LOCALUSER_NAME.equals(mechanismName)) {
            if (!POSIX_SUPPORTED) {
                log.localUserDisabledNoPosix();
                return null;
            }
            String challengePath = (String) properties.get(LocalUserAuthenticationMechanism.LOCAL_USER_CHALLENGE_PATH);
            String defaultUser = (String) properties.get(LocalUserAuthenticationMechanism.DEFAULT_USER);
            return new LocalUserAuthenticationMechanism(callbackHandler, challengePath, defaultUser,
                  pendingChallenges, sessionTokens, cleanupExecutor);
        }

        return null;
    }

    @Override
    public void shutdown() {
        pendingChallenges.clear();
        sessionTokens.clear();
    }
}
