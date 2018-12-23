package com.unis.javautil;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PropertiesUtil {
	Properties properties=null;
	public PropertiesUtil(){
		
	}
	public PropertiesUtil(String file){
		try {

			String path = PropertiesUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			String filePath =path.substring(0, path.lastIndexOf("/")+1);
			FileInputStream fis = new FileInputStream(filePath+file);
			properties=new Properties();
			properties.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public  String getString(String key){
		return properties.getProperty(key).trim();
	}
	public int getInt(String key){
		return Integer.valueOf(properties.getProperty(key).trim());
	}
	public double getDouble(String key){
		return Double.valueOf(properties.getProperty(key).trim());
	}
	public boolean getBoolean(String key){
		return Boolean.valueOf(properties.getProperty(key).trim());
	}
	public List<String> getList(String key,String splitChar){
		String str=properties.getProperty(key).trim();
		String[] tmp = str.split(splitChar);
		return Arrays.asList(tmp);
	}
	
}
