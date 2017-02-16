package org.infinispan.server.test.task.servertask;

import java.io.Serializable;

/**
 * A custom mojo which is used while remote server task execution.
 *
 * @author amanukya
 */
public class Greeting implements Serializable {
    private String greeting;

    public Greeting(String ahoj) {
        this.greeting = ahoj;
    }

    public String getGreeting() {
        return greeting;
    }

    public void setGreeting(String greeting) {
        this.greeting = greeting;
    }
}
