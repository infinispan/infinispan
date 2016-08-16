package org.infinispan.test.integration.security.tasks;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ALLOW_RESOURCE_SERVICE_RESTART;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.COMPOSITE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_HEADERS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REMOVE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ROLLBACK_ON_RUNTIME_FAILURE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.STEPS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jboss.as.arquillian.api.ServerSetupTask;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.test.integration.security.common.Constants;
import org.jboss.as.test.integration.security.common.CoreUtils;
import org.jboss.as.test.integration.security.common.config.AuthnModule;
import org.jboss.as.test.integration.security.common.config.JSSE;
import org.jboss.as.test.integration.security.common.config.JaspiAuthn;
import org.jboss.as.test.integration.security.common.config.LoginModuleStack;
import org.jboss.as.test.integration.security.common.config.SecureStore;
import org.jboss.as.test.integration.security.common.config.SecurityDomain;
import org.jboss.as.test.integration.security.common.config.SecurityModule;
import org.jboss.dmr.ModelNode;
import org.jboss.logging.Logger;

/**
 * {@link ServerSetupTask} instance for security domain setup. It supports JSSE configuration, JASPI authentication
 * configuration and stacks of login-modules (classic authentication), policy-modules and (role-)mapping-modules.
 *
 * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
public abstract class AbstractSecurityDomainsServerSetupTask implements ServerSetupTask {

   private static final Logger LOGGER = Logger.getLogger(AbstractSecurityDomainsServerSetupTask.class);

   private static final String ROLE = "role"; // The type attribute value of mapping-modules used for role assignment.
   private static final String SUBSYSTEM_SECURITY = "security"; // The SUBSYSTEM_SECURITY

   protected ManagementClient managementClient;

   private SecurityDomain[] securityDomains;

   public void setup(final ManagementClient managementClient, String containerId) throws Exception {
      this.managementClient = managementClient;
      securityDomains = getSecurityDomains();

      if (securityDomains == null || securityDomains.length == 0) {
         LOGGER.warn("Empty security domain configuration.");
         return;
      }

      final List<ModelNode> updates = new LinkedList<ModelNode>();
      for (final SecurityDomain securityDomain : securityDomains) {
         final String securityDomainName = securityDomain.getName();
         if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Adding security domain " + securityDomainName);
         }
         final ModelNode compositeOp = new ModelNode();
         compositeOp.get(OP).set(COMPOSITE);
         compositeOp.get(OP_ADDR).setEmptyList();
         ModelNode steps = compositeOp.get(STEPS);

         PathAddress opAddr = PathAddress.pathAddress()
               .append(SUBSYSTEM, SUBSYSTEM_SECURITY)
               .append(Constants.SECURITY_DOMAIN, securityDomainName);
         ModelNode op = Util.createAddOperation(opAddr);
         if (StringUtils.isNotEmpty(securityDomain.getCacheType())) {
            op.get(Constants.CACHE_TYPE).set(securityDomain.getCacheType());
         }
         steps.add(op);

         //only one can occur - authenticationType or authenticationJaspiType
         final boolean authNodeAdded = createSecurityModelNode(Constants.AUTHENTICATION, Constants.LOGIN_MODULE, Constants.FLAG,
                                                               Constants.REQUIRED, securityDomain.getLoginModules(), securityDomainName, steps);
         if (!authNodeAdded) {
            final List<ModelNode> jaspiAuthnNodes = createJaspiAuthnNodes(securityDomain.getJaspiAuthn(), securityDomain.getName());
            if (jaspiAuthnNodes != null) {
               for (ModelNode node : jaspiAuthnNodes) {
                  steps.add(node);
               }
            }
         }
         createSecurityModelNode(Constants.AUTHORIZATION, Constants.POLICY_MODULE, Constants.FLAG, Constants.REQUIRED, securityDomain.getAuthorizationModules(), securityDomainName, steps);
         createSecurityModelNode(Constants.MAPPING, Constants.MAPPING_MODULE, Constants.TYPE, ROLE, securityDomain.getMappingModules(), securityDomainName, steps);

         final ModelNode jsseNode = createJSSENode(securityDomain.getJsse(), securityDomain.getName());
         if (jsseNode != null) {
            steps.add(jsseNode);
         }
         updates.add(compositeOp);
      }
      CoreUtils.applyUpdates(updates, managementClient.getControllerClient());
   }

   public void tearDown(ManagementClient managementClient, String containerId) throws Exception {
      if (securityDomains == null || securityDomains.length == 0) {
         LOGGER.warn("Empty security domain configuration.");
         return;
      }

      final List<ModelNode> updates = new ArrayList<ModelNode>();
      for (final SecurityDomain securityDomain : securityDomains) {
         final String domainName = securityDomain.getName();
         if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Removing security domain " + domainName);
         }
         final ModelNode op = new ModelNode();
         op.get(OP).set(REMOVE);
         op.get(OP_ADDR).add(SUBSYSTEM, "security");
         op.get(OP_ADDR).add(Constants.SECURITY_DOMAIN, domainName);
         // Don't rollback when the AS detects the war needs the module
         op.get(OPERATION_HEADERS, ROLLBACK_ON_RUNTIME_FAILURE).set(false);
         op.get(OPERATION_HEADERS, ALLOW_RESOURCE_SERVICE_RESTART).set(true);
         updates.add(op);
      }
      CoreUtils.applyUpdates(updates, managementClient.getControllerClient());

      this.managementClient = null;
   }

   /**
    * Creates authenticaton=>jaspi node and its child nodes.
    *
    * @param securityConfigurations
    * @return
    */
   private List<ModelNode> createJaspiAuthnNodes(JaspiAuthn securityConfigurations, String domainName) {
      if (securityConfigurations == null) {
         LOGGER.info("No security configuration for JASPI module.");
         return null;
      }
      if (securityConfigurations.getAuthnModules() == null || securityConfigurations.getAuthnModules().length == 0
            || securityConfigurations.getLoginModuleStacks() == null
            || securityConfigurations.getLoginModuleStacks().length == 0) {
         throw new IllegalArgumentException("Missing mandatory part of JASPI configuration in the security domain.");
      }

      final List<ModelNode> steps = new ArrayList<ModelNode>();

      PathAddress domainAddress = PathAddress.pathAddress()
            .append(SUBSYSTEM, SUBSYSTEM_SECURITY)
            .append(Constants.SECURITY_DOMAIN, domainName);
      PathAddress jaspiAddress = domainAddress.append(Constants.AUTHENTICATION, Constants.JASPI);
      steps.add(Util.createAddOperation(jaspiAddress));

      for (final AuthnModule config : securityConfigurations.getAuthnModules()) {
         LOGGER.info("Adding auth-module: " + config);
         final ModelNode securityModuleNode = Util.createAddOperation(jaspiAddress.append(Constants.AUTH_MODULE, config.getName()));
         steps.add(securityModuleNode);
         securityModuleNode.get(ModelDescriptionConstants.CODE).set(config.getName());
         if (config.getFlag() != null) {
            securityModuleNode.get(Constants.FLAG).set(config.getFlag());
         }
         if (config.getModule() != null) {
            securityModuleNode.get(Constants.MODULE).set(config.getModule());
         }
         if (config.getLoginModuleStackRef() != null) {
            securityModuleNode.get(Constants.LOGIN_MODULE_STACK_REF).set(config.getLoginModuleStackRef());
         }
         Map<String, String> configOptions = config.getOptions();
         if (configOptions == null) {
            LOGGER.info("No module options provided.");
            configOptions = Collections.emptyMap();
         }
         final ModelNode moduleOptionsNode = securityModuleNode.get(Constants.MODULE_OPTIONS);
         for (final Map.Entry<String, String> entry : configOptions.entrySet()) {
            final String optionName = entry.getKey();
            final String optionValue = entry.getValue();
            moduleOptionsNode.add(optionName, optionValue);
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Adding module option [" + optionName + "=" + optionValue + "]");
            }
         }
      }
      //Unable to use securityComponentNode.get(OPERATION_HEADERS).get(ALLOW_RESOURCE_SERVICE_RESTART).set(true) because the login-module-stack is empty


      for (final LoginModuleStack lmStack : securityConfigurations.getLoginModuleStacks()) {
         PathAddress lmStackAddress = jaspiAddress.append(Constants.LOGIN_MODULE_STACK, lmStack.getName());
         steps.add(Util.createAddOperation(lmStackAddress));

         for (final SecurityModule config : lmStack.getLoginModules()) {
            final String code = config.getName();
            final ModelNode securityModuleNode = Util.createAddOperation(lmStackAddress.append(Constants.LOGIN_MODULE, code));

            final String flag = StringUtils.defaultIfEmpty(config.getFlag(), Constants.REQUIRED);
            securityModuleNode.get(ModelDescriptionConstants.CODE).set(code);
            securityModuleNode.get(Constants.FLAG).set(flag);
            if (LOGGER.isInfoEnabled()) {
               LOGGER.info("Adding JASPI login module stack [code=" + code + ", flag=" + flag + "]");
            }
            Map<String, String> configOptions = config.getOptions();
            if (configOptions == null) {
               LOGGER.info("No module options provided.");
               configOptions = Collections.emptyMap();
            }
            final ModelNode moduleOptionsNode = securityModuleNode.get(Constants.MODULE_OPTIONS);
            for (final Map.Entry<String, String> entry : configOptions.entrySet()) {
               final String optionName = entry.getKey();
               final String optionValue = entry.getValue();
               moduleOptionsNode.add(optionName, optionValue);
               if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Adding module option [" + optionName + "=" + optionValue + "]");
               }
            }
            securityModuleNode.get(OPERATION_HEADERS).get(ALLOW_RESOURCE_SERVICE_RESTART).set(true);
            steps.add(securityModuleNode);
         }

      }

      return steps;
   }

   /**
    * Creates a {@link ModelNode} with the security component configuration. If the securityConfigurations array is
    * empty or null, then null is returned.
    *
    * @param securityComponent name of security component (e.g. {@link Constants#AUTHORIZATION})
    * @param subnodeName       name of the security component subnode, which holds module configurations (e.g. {@link
    *                          Constants#POLICY_MODULES})
    * @param flagAttributeName name of attribute to which the value of {@link SecurityModule#getFlag()} is set
    * @param flagDefaultValue  default value for flagAttributeName attr.
    * @param securityModules   configurations
    * @return ModelNode instance or null
    */
   private boolean createSecurityModelNode(String securityComponent, String subnodeName, String flagAttributeName,
                                           String flagDefaultValue, final SecurityModule[] securityModules, String domainName, ModelNode operations) {
      if (securityModules == null || securityModules.length == 0) {
         if (LOGGER.isInfoEnabled()) {
            LOGGER.info("No security configuration for " + securityComponent + " module.");
         }
         return false;
      }
      PathAddress address = PathAddress.pathAddress()
            .append(SUBSYSTEM, SUBSYSTEM_SECURITY)
            .append(Constants.SECURITY_DOMAIN, domainName)
            .append(securityComponent, Constants.CLASSIC);
      operations.add(Util.createAddOperation(address));

      for (final SecurityModule config : securityModules) {
         final String code = config.getName();
         final ModelNode securityModuleNode = Util.createAddOperation(address.append(subnodeName, code));

         final String flag = StringUtils.defaultIfEmpty(config.getFlag(), flagDefaultValue);
         securityModuleNode.get(ModelDescriptionConstants.CODE).set(code);
         securityModuleNode.get(flagAttributeName).set(flag);
         Map<String, String> configOptions = config.getOptions();
         if (configOptions == null) {
            LOGGER.info("No module options provided.");
            configOptions = Collections.emptyMap();
         }
         if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Adding " + securityComponent + " module [code=" + code + ", " + flagAttributeName + "=" + flag
                              + ", options = " + configOptions + "]");
         }
         final ModelNode moduleOptionsNode = securityModuleNode.get(Constants.MODULE_OPTIONS);
         for (final Map.Entry<String, String> entry : configOptions.entrySet()) {
            final String optionName = entry.getKey();
            final String optionValue = entry.getValue();
            moduleOptionsNode.add(optionName, optionValue);
         }
         securityModuleNode.get(OPERATION_HEADERS).get(ALLOW_RESOURCE_SERVICE_RESTART).set(true);
         operations.add(securityModuleNode);
      }
      return true;
   }

   private ModelNode createJSSENode(final JSSE jsse, String domainName) {
      if (jsse == null) {
         if (LOGGER.isInfoEnabled()) {
            LOGGER.info("No security configuration for JSSE module.");
         }
         return null;
      }
      final ModelNode securityComponentNode = new ModelNode();
      securityComponentNode.get(OP).set(ADD);
      securityComponentNode.get(OP_ADDR).add(SUBSYSTEM, SUBSYSTEM_SECURITY);
      securityComponentNode.get(OP_ADDR).add(Constants.SECURITY_DOMAIN, domainName);

      securityComponentNode.get(OP_ADDR).add(Constants.JSSE, Constants.CLASSIC);
      addSecureStore(jsse.getTrustStore(), Constants.TRUSTSTORE, securityComponentNode);
      addSecureStore(jsse.getKeyStore(), Constants.KEYSTORE, securityComponentNode);
      securityComponentNode.get(OPERATION_HEADERS).get(ALLOW_RESOURCE_SERVICE_RESTART).set(true);
      return securityComponentNode;
   }

   private void addSecureStore(SecureStore secureStore, String storeName, ModelNode jsseNode) {
      if (secureStore == null) {
         return;
      }
      if (secureStore.getUrl() != null) {
         jsseNode.get(storeName, Constants.URL).set(secureStore.getUrl().toExternalForm());
      }
      if (secureStore.getPassword() != null) {
         jsseNode.get(storeName, Constants.PASSWORD).set(secureStore.getPassword());
      }
      if (secureStore.getType() != null) {
         jsseNode.get(storeName, Constants.TYPE).set(secureStore.getType());
      }
   }

   protected abstract SecurityDomain[] getSecurityDomains() throws Exception;
}