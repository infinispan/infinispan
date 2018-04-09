package org.infinispan.server.jgroups.security;

import static org.wildfly.security.password.interfaces.DigestPassword.ALGORITHM_DIGEST_MD5;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.DIGEST_MD5;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.EXTERNAL;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.GSSAPI;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.PLAIN;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.server.jgroups.logging.JGroupsLogger;
import org.jboss.as.core.security.RealmUser;
import org.jboss.as.core.security.SubjectUserInfo;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.RealmConfigurationConstants;
import org.jboss.as.domain.management.SecurityRealm;
import org.wildfly.security.auth.callback.AvailableRealmsCallback;
import org.wildfly.security.auth.callback.CredentialCallback;
import org.wildfly.security.password.spec.DigestPasswordAlgorithmSpec;
import org.wildfly.security.sasl.WildFlySasl;

/**
 * RealmAuthorizationCallbackHandler. A {@link CallbackHandler} for JGroups which piggybacks on the
 * realm-provided {@link AuthorizingCallbackHandler}s and provides additional role validation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class RealmAuthorizationCallbackHandler implements CallbackHandler {
    private final String mechanismName;
    private final SecurityRealm realm;
    private final String clusterRole;
    private String[] realmList;

    private static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.wildfly.security.sasl.digest.pre_digested";

    public RealmAuthorizationCallbackHandler(SecurityRealm realm, String mechanismName, String clusterRole, Map<String, String> mechanismProperties) {
        this.realm = realm;
        this.mechanismName = mechanismName;
        this.clusterRole = clusterRole;
        tunePropsForMech(mechanismProperties);
    }

    private void tunePropsForMech(Map<String, String> mechanismProperties) {
        if (DIGEST_MD5.equals(mechanismName)) {
            String realmStr = mechanismProperties.get(WildFlySasl.REALM_LIST);
            realmList = realmStr == null ? new String[] {realm.getName()} : realmStr.split(" ");

            Map<String, String> mechConfig = realm.getMechanismConfig(AuthMechanism.DIGEST);
            boolean plainTextDigest = true;
            if (mechConfig.containsKey(RealmConfigurationConstants.DIGEST_PLAIN_TEXT)) {
                plainTextDigest = Boolean.parseBoolean(mechConfig.get(RealmConfigurationConstants.DIGEST_PLAIN_TEXT));
            }
            if (!plainTextDigest) {
                mechanismProperties.put(SASL_OPT_PRE_DIGESTED_PROPERTY, "true");
            }
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        // We have to provide the available realms via this callback
        // Ideally we would utilise org.wildfly.security.sasl.util.AvailableRealmsSaslServerFactory, however as we can't
        // pass the SaslServerFactory impl to JGroups we must do it here instead.
        ArrayList<Callback> list = new ArrayList<>(Arrays.asList(callbacks));
        Iterator<Callback> it = list.iterator();
        CredentialCallback cb = null;
        while (it.hasNext()) {
            Callback callback = it.next();
            if (callback instanceof AvailableRealmsCallback) {
                ((AvailableRealmsCallback) callback).setRealmNames(realmList);
                it.remove();
            } else if (callback instanceof CredentialCallback) {
                cb = (CredentialCallback) callback;
            }
        }

        // If the only callback was AvailableRealmsCallback, we must not pass it to the AuthorizingCallbackHandler
        if (!list.isEmpty()) {
            if (cb != null && cb.getAlgorithm().equals(ALGORITHM_DIGEST_MD5)) {
                // It's necessary to add the NameCallback with the CredentialCallback, otherwise a UserNotFoundException is thrown
                DigestPasswordAlgorithmSpec spec = (DigestPasswordAlgorithmSpec) cb.getParameterSpec();
                list.add(new NameCallback("User", spec.getUsername()));
                callbacks = list.toArray(new Callback[list.size()]);
            }
            getMechCallbackHandler().handle(callbacks);
        }
    }

    private AuthorizingCallbackHandler getMechCallbackHandler() {
        if (PLAIN.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
        } else if (DIGEST_MD5.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.DIGEST));
        } else if (GSSAPI.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
        } else if (EXTERNAL.equals(mechanismName)) {
            return new DelegatingRoleAwareAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.CLIENT_CERT));
        } else {
            throw new IllegalArgumentException("Unsupported mech " + mechanismName);
        }
    }

    SubjectUserInfo validateSubjectRole(SubjectUserInfo subjectUserInfo) {
        for(Principal principal : subjectUserInfo.getPrincipals()) {
            if (clusterRole.equals(principal.getName())) {
                return subjectUserInfo;
            }
        }
        throw JGroupsLogger.ROOT_LOGGER.unauthorizedNodeJoin(subjectUserInfo.getUserName());
    }

    class DelegatingRoleAwareAuthorizingCallbackHandler implements AuthorizingCallbackHandler {
        private final AuthorizingCallbackHandler delegate;

        DelegatingRoleAwareAuthorizingCallbackHandler(AuthorizingCallbackHandler acbh) {
            this.delegate = acbh;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            AuthorizeCallback acb = findCallbackHandler(AuthorizeCallback.class, callbacks);
            if (acb != null) {
                String authenticationId = acb.getAuthenticationID();
                String authorizationId = acb.getAuthorizationID();
                acb.setAuthorized(authenticationId.equals(authorizationId));
                int realmSep = authorizationId.indexOf('@');
                RealmUser realmUser = realmSep < 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep+1), authorizationId.substring(0, realmSep));
                List<Principal> principals = new ArrayList<>();
                principals.add(realmUser);
                createSubjectUserInfo(principals);
            } else {
                delegate.handle(callbacks);
            }
        }

        @Override
        public SubjectUserInfo createSubjectUserInfo(Collection<Principal> principals) throws IOException {
            // The call to the delegate will supplement the user with additional role information
            SubjectUserInfo subjectUserInfo = delegate.createSubjectUserInfo(principals);
            return validateSubjectRole(subjectUserInfo);
        }
    }

    private static <T extends Callback> T findCallbackHandler(Class<T> klass, Callback[] callbacks) {
        for (Callback callback : callbacks) {
            if (klass.isInstance(callback))
                return (T) callback;
        }
        return null;
    }
}
