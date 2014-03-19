package org.infinispan.persistence.jpa.entity;

import java.io.Serializable;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Entity
public class Vehicle implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3726691096915715402L;

	@EmbeddedId
	private VehicleId id;
	
	private String color;

	public VehicleId getId() {
		return id;
	}

	public void setId(VehicleId id) {
		this.id = id;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((color == null) ? 0 : color.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Vehicle other = (Vehicle) obj;
		if (color == null) {
			if (other.color != null)
				return false;
		} else if (!color.equals(other.color))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Vehicle [id=" + id + ", color=" + color + "]";
	}
}
