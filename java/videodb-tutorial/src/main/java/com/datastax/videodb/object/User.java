package com.datastax.videodb.object;

import java.util.Date;
import java.util.UUID;

public class User {

	public User() {
		super();

	}

	public User(String username, String firstname, String lastname,
			String email, String password, Date created_date,
			int total_credits, UUID credit_change_date) {
		super();
		this.username = username;
		this.firstname = firstname;
		this.lastname = lastname;
		this.email = email;
		this.password = password;
		this.created_date = created_date;
		this.total_credits = total_credits;
		this.credit_change_date = credit_change_date;
	}

	/**
	 * @param args
	 */

	private String username;
	private String firstname;
	private String lastname;
	private String email;
	private String password;
	private Date created_date;
	private int total_credits;
	private UUID credit_change_date;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Date getCreated_date() {
		return created_date;
	}

	public void setCreated_date(Date created_date) {
		this.created_date = created_date;
	}

	public int getTotal_credits() {
		return total_credits;
	}

	public void setTotal_credits(int total_credits) {
		this.total_credits = total_credits;
	}

	public UUID getCredit_change_date() {
		return credit_change_date;
	}

	public void setCredit_change_date(UUID credit_change_date) {
		this.credit_change_date = credit_change_date;
	}

	public String toString() {
		return "User [username=" + username + ", firstname=" + firstname
				+ ", lastname=" + lastname + ", email=" + email + ", password="
				+ password + ", created_date=" + created_date
				+ ", total_credits=" + total_credits + ", credit_change_date="
				+ credit_change_date + "]";
	}

}
