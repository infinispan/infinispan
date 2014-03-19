package org.infinispan.persistence.jpa.entity;

import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Embeddable
public class VehicleId implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3684882454815768434L;
	
	private String state;
	private String licensePlate;
	
	public VehicleId() {
	}
	
	public VehicleId(String state, String licensePlate) {
		this.state = state;
		this.licensePlate = licensePlate;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((licensePlate == null) ? 0 : licensePlate.hashCode());
		result = prime * result + ((state == null) ? 0 : state.hashCode());
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
		VehicleId other = (VehicleId) obj;
		if (licensePlate == null) {
			if (other.licensePlate != null)
				return false;
		} else if (!licensePlate.equals(other.licensePlate))
			return false;
		if (state == null) {
			if (other.state != null)
				return false;
		} else if (!state.equals(other.state))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "VehicleId [state=" + state + ", licensePlate=" + licensePlate
				+ "]";
	}
}
