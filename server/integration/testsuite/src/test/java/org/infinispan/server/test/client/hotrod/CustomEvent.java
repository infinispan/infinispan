package org.infinispan.server.test.client.hotrod;

import java.io.Serializable;

public class CustomEvent implements Serializable {
    final Integer key;
    final String value;
    CustomEvent(Integer key, String value) {
        this.key = key;
        this.value = value;
    }
}
