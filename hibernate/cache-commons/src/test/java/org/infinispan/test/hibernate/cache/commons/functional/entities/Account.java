package org.infinispan.test.hibernate.cache.commons.functional.entities;

import java.io.Serializable;

/**
 * Comment
 *
 * @author Brian Stansberry
 */
public class Account implements Serializable {

	private static final long serialVersionUID = 1L;

	private Integer id;
	private long version;
	private AccountHolder accountHolder;
	private Integer balance;
	private String branch;

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

	public AccountHolder getAccountHolder() {
		return accountHolder;
	}

	public void setAccountHolder(AccountHolder accountHolder) {
		this.accountHolder = accountHolder;
	}

	public Integer getBalance() {
		return balance;
	}

	public void setBalance(Integer balance) {
		this.balance = balance;
	}

	public String getBranch() {
		return branch;
	}

	public void setBranch(String branch) {
		this.branch = branch;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof Account))
			return false;
		Account acct = (Account) obj;
		if (!safeEquals(id, acct.id))
			return false;
		if (!safeEquals(branch, acct.branch))
			return false;
		if (!safeEquals(balance, acct.balance))
			return false;
		if (!safeEquals(accountHolder, acct.accountHolder))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = 17;
		result = result * 31 + safeHashCode(id);
		result = result * 31 + safeHashCode(branch);
		result = result * 31 + safeHashCode(balance);
		result = result * 31 + safeHashCode(accountHolder);
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getClass().getName());
		sb.append("[id=");
		sb.append(id);
		sb.append(",branch=");
		sb.append(branch);
		sb.append(",balance=");
		sb.append(balance);
		sb.append(",accountHolder=");
		sb.append(accountHolder);
		sb.append("]");
		return sb.toString();
	}

	private static int safeHashCode(Object obj) {
		return obj == null ? 0 : obj.hashCode();
	}

	private static boolean safeEquals(Object a, Object b) {
		return (a == b || (a != null && a.equals(b)));
	}

}
