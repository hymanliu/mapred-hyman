package com.hyman.hadoop.three;

public class User {
	private String userId;
	private String phone;
	private String username;
	private String age;
	
	
	public User(String userId, String phone) {
		super();
		this.userId = userId;
		this.phone = phone;
	}

	
	public User(String userId, String username, String age) {
		super();
		this.userId = userId;
		this.username = username;
		this.age = age;
	}

	public User(String userId, String phone, String username, String age) {
		super();
		this.userId = userId;
		this.phone = phone;
		this.username = username;
		this.age = age;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return this.userId+"\t"+this.username+"\t"+this.phone+"\t"+this.age;
	}
	
}
