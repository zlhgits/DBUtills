package com.znbill.dbpool;

/**
 * DBC操作工具类
 * @author zlh
 * @date 2015-12-29
 *
 */

import java.io.IOException;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

/* 
 * 连接池的管理类，负责读取配置连接池的文件，并创建连接池 
 * 从连接池中获取，释放连接 
 */
public class DBConnectionManager {

	/*
	 * 唯一数据库连接池管理实例类 使用单例模式创建
	 */
	private static DBConnectionManager instance;

	/*
	 * 连接池的集合
	 */
	private Hashtable<String, DBConnectionPool> pools = new Hashtable<String, DBConnectionPool>();
	private static String poolName = "JDBC_POOL"; // 连接池名字

	public static synchronized DBConnectionManager getInstance() {
		if (instance == null) {
			instance = new DBConnectionManager ();
		}
		return instance;
	}

	/*
	 * 只允许内部实例化管理类
	 */
	private DBConnectionManager() {
		this.init();
	}

	/*
	 * 加载驱动程序
	 */
	private void init() {
		Properties prop = new Properties (  );
		try {
			prop.load ( DBConnectionManager.class.getClassLoader ().getResourceAsStream ( "jdbc.properties" ) );

			String driverName = prop.getProperty("driver");
			String username = prop.getProperty("user");
			String password = prop.getProperty("passwd");
			String url = prop.getProperty("url");
			int maxConn = Integer.parseInt ( prop.getProperty ( "size" ) );

			DBConnectionPool pool = new DBConnectionPool(driverName, url,
					username, password, maxConn);
			pools.put(poolName, pool);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/*
	 * 根据连接池的名字得到一个连接
	 */
	public Connection getConnection() {
		DBConnectionPool pool = null;
		Connection conn = null;
		pool = pools.get(poolName);
		try {
			conn = pool.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	/*
	 * 释放一个连接
	 */
	public synchronized void freeConnection(Connection conn) {
		DBConnectionPool pool = pools.get(poolName);
		if (pool != null) {
			pool.freeConnection(conn);
		}
	}

	/*
	 * 释放所有连接
	 */
	public synchronized void release() {
		Enumeration<DBConnectionPool> allpools = pools.elements();
		while (allpools.hasMoreElements()) {
			DBConnectionPool pool = allpools.nextElement();
			if (pool != null) {
				pool.release();
			}
		}
		pools.clear();
	}

}
