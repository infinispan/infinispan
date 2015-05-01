package org.infinispan.test.integration.as.testframework;

public class ArquillianH2Extension implements org.jboss.arquillian.core.spi.LoadableExtension {

	@Override
	public void register(ExtensionBuilder builder) {
		builder.observer(H2DatabaseLifecycleManager.class);
	}

}
