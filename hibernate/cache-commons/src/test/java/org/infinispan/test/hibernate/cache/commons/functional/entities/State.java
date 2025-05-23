
//$Id$
package org.infinispan.test.hibernate.cache.commons.functional.entities;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Transient;

/**
 * @author Emmanuel Bernard
 */
@Entity
public class State {
	@Id
	@GeneratedValue
	private Integer id;
	@Transient
	private long version;
	private String name;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
