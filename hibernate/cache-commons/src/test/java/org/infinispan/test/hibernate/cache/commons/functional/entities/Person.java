package org.infinispan.test.hibernate.cache.commons.functional.entities;

import java.io.Serializable;

import jakarta.persistence.Cacheable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Transient;

/**
 * Test class using EmbeddedId
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
@Cacheable
public class Person implements Serializable {
    @EmbeddedId
    Name name;

    int age;

    @Transient
    long version;

    public Person() {}

    public Person(String firstName, String lastName, int age) {
        name = new Name(firstName, lastName);
        this.age = age;
    }

    public Name getName() {
        return name;
    }

    public void setName(Name name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
