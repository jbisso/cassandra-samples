package com.datastax.videodb.pojo;

import java.util.Date;
import java.util.UUID;

public class User {

	public User() {
		super();

	}

	public User(String userName, String firstName, String lastName,
			String email, String password, Date createdDate,
			int totalCredits, UUID creditChangeDate) {
		super();
		this.userName = userName;
		this.firstName = firstName;
		this.lastName = lastName;
		this.email = email;
		this.password = password;
		this.createdDate = createdDate;
		this.totalCredits = totalCredits;
		this.creditChangeDate = creditChangeDate;
	}

	/**
	 * @param args
	 */

	private String userName;
	private String firstName;
	private String lastName;
	private String email;
	private String password;
	private Date createdDate;
	private int totalCredits;
	private UUID creditChangeDate;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
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

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}

	public int getTotalCredits() {
		return totalCredits;
	}

	public void setTotalCredits(int totalCredits) {
		this.totalCredits = totalCredits;
	}

	public UUID getCreditChangeDate() {
		return creditChangeDate;
	}

	public void setCreditChangeDate(UUID creditChangeDate) {
		this.creditChangeDate = creditChangeDate;
	}

	public String toString() {
		return "User [username=" + userName + ", firstname=" + firstName
				+ ", lastname=" + lastName + ", email=" + email + ", password="
				+ password + ", created_date=" + createdDate
				+ ", total_credits=" + totalCredits + ", credit_change_date="
				+ creditChangeDate + "]";
	}

}
