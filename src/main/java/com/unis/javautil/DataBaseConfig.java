package com.unis.javautil;



public class DataBaseConfig {
	
	
	private String driverClass;
	private String userName;
	private String passWord;
    private String url;
    private DataBaseConfig dataBaseConfig=new DataBaseConfig();

	public String getDriverClass() {
		return driverClass;
	}

	public DataBaseConfig setDriverClass(String driverClass) {
		this.driverClass = driverClass;
		return this;
	}
	
	public String getUserName() {
		return userName;
	}

	public DataBaseConfig setUserName(String userName) {
		this.userName = userName;
		return this;
	}

	public String getPassWord() {
		return passWord;
	}

	public DataBaseConfig setPassWord(String passWord) {
		this.passWord = passWord;
		return this;
	}

	public String getUrl() {
		return url;
	}

	public DataBaseConfig setUrl(String url) {
		this.url = url;
		return this;
	}
	public DataBaseConfig getDataBaseConfig(){
		return this.dataBaseConfig;
	}
	
	
}
