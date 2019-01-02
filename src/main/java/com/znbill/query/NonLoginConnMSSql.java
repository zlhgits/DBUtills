package com.znbill.query;

import java.sql.*;

/**
 * SQLSERVER数据库无密码模式登录
 * Author zlh
 * Date 2018-12-27
 * Version 1.0
 */
public class NonLoginConnMSSql {
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    public ResultSet getMSSqlRs() {

        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        //windows集成模式连接
        String url = "jdbc:sqlserver://192.168.31.133:1433;databaseName=UFDATA_001_2018;integratedSecurity=true;";
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select top 10 * from CA_RptSys");

            while(rs.next()){
                System.out.println(rs.getString("a")+"\t"+rs.getString("b"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally{
            try {
                if (stmt!=null){stmt.close();}
                if (conn!=null){conn.close();}
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return rs;

    }
}
