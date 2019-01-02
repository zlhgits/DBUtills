package com.znbill.query;

import com.znbill.dbpool.DBConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * CRUD，暂时只有插入
 * Author zlh
 * Date 2018-12-27
 * Version 1.0
 */
public class DBUtil {
    private Logger LOG = LoggerFactory.getLogger ( DBUtil.class );
    private DBConnectionManager dbm = null;
    private Connection conn = null;
    private ResultSet rs = null;

    /**
     * 批量插入数据
     *
     * @return
     */
    public Boolean insertIntoTable() {
        Boolean flag = true;

        PreparedStatement ps = null;
        String sql = "insert into kafka_failedmsg(" +
                "kafka_topic,kafka_key,kafka_value,kafka_errmsg) " +
                "values(?,?,?,?)";
        try {
            ps = conn.prepareStatement ( sql );
            for (int i = 0; i < 1000; i++) {
                String tmp = "在他12岁的时候，他已能设计电子游戏机，帮忙邻居修理收割机。大学时期在天文系担任程式开发工读生。";
                String uuid = UUID.randomUUID ().toString () + tmp;
                ps.setString ( 1, uuid );
                ps.setString ( 2, uuid );
                ps.setString ( 3, uuid );
                ps.setString ( 4, uuid );
                ps.addBatch ();
            }
            ps.executeBatch ();
            conn.commit ();
        } catch (SQLException e1) {
            e1.printStackTrace ();
        }

        return flag;
    }

    //计算吞吐量
    public void dbMain(String arg) {
        try {
            dbm = DBConnectionManager.getInstance ();
            conn = dbm.getConnection ();
            Boolean b = true;
            int flag = Integer.parseInt ( arg );
            int run = 0;
            while (b) {
                long s = System.currentTimeMillis ();
                b = insertIntoTable ();
                long end = System.currentTimeMillis ();
                LOG.info ( "L每秒生产数据条数：" + String.valueOf ( 1000 * 1000 / (end - s) ) );
                System.out.println ( "S每秒生产数据条数：" + String.valueOf ( 1000 * 1000 / (end - s) ) );
                run += 1;
                if (run > flag) {
                    break;
                }
            }
        } catch (Exception e) {
            if (dbm != null) {
                dbm.release ();
            }
        }
    }

}
