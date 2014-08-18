package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientListener;

@ClientListener(converterFactoryName = "non-existing-test-converter-factory")
public class NonExistingConverterFactoryCustomEventListener extends CustomEventListener {

}
