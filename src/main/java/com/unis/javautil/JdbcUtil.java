package com.unis.javautil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.BasicDataSource;



/**
 * @Description: JDBC操作元数据示例-- DatabaseMetaData接口
 * @CreateTime:
 * @author:
 * @version
 */
public class JdbcUtil {

	// 数据库连接池
	private static BasicDataSource dbcp;

	// 为不同线程管理连接
	private static ThreadLocal<Connection> tl;
	private static PropertiesUtil prop;

	static {
		prop = new PropertiesUtil("jdbc.properties");
		// 一、初始化连接池
		dbcp = new BasicDataSource();
		// 设置驱动 (Class.forName())
		dbcp.setDriverClassName(prop.getString("jdbc.driverClass"));
		// 设置url
		dbcp.setUrl(prop.getString("jdbc.url"));
		// 设置数据库用户名
		dbcp.setUsername(prop.getString("jdbc.username"));
		// 设置数据库密码
		dbcp.setPassword(prop.getString("jdbc.password"));
		// 初始连接数量
		dbcp.setInitialSize(prop.getInt("initsize"));
		// 连接池允许的最大连接数
		dbcp.setMaxActive(prop.getInt("maxactive"));
		// 设置最大等待时间
		dbcp.setMaxWait(prop.getInt("maxwait"));
		// 设置最小空闲数
		dbcp.setMinIdle(prop.getInt("minidle"));
		// 设置最大空闲数
		dbcp.setMaxIdle(prop.getInt("maxidle"));
		// 初始化线程本地
		tl = new ThreadLocal<Connection>();
		
		
	}

	public synchronized static Connection getConnection(DataBaseConfig dataBaseConfig) {
		Connection conn = null;
		try {
			Class.forName(dataBaseConfig.getDriverClass());
			DriverManager.setLoginTimeout(5);
			conn = DriverManager.getConnection(dataBaseConfig.getUrl(), dataBaseConfig.getUserName(),
					dataBaseConfig.getPassWord());
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static PropertiesUtil getProp(){
		return prop;
	}
	/**
	 * 获取数据库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		/*
		 * 通过连接池获取一个空闲连接
		 */
		Connection conn = dbcp.getConnection();
		tl.set(conn);
		return conn;
	}

	/**
	 * 关闭数据库连接
	 */
	public static void closeConnection() {
		try {
			Connection conn = tl.get();
			if (conn != null) {
				/*
				 * 通过连接池获取的Connection 的close()方法实际上并没有将 连接关闭，而是将该链接归还。
				 */
				conn.close();
				tl.remove();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 关闭连接
	public static void close(Object o) {
		if (o == null) {
			return;
		}
		if (o instanceof ResultSet) {
			try {
				((ResultSet) o).close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (o instanceof Statement) {
			try {
				((Statement) o).close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (o instanceof Connection) {
			Connection c = (Connection) o;
			try {
				if (!c.isClosed()) {
					c.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void close(ResultSet rs, Statement stmt, Connection conn) {
		close(rs);
		close(stmt);
		close(conn);
	}

	public static void close(ResultSet rs, Connection conn) {
		close(rs);
		close(conn);
	}
}