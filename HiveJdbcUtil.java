
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveJdbcUtil {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://127.0.0.1:10000/hadoop_db";
    private static String user = "root";
    private static String password = "";
 
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    static {
        try {
        	Class.forName(driverName);
        	conn = DriverManager.getConnection(url, user, password);
        	System.out.println("============"+conn);
			//stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    // 加载驱动、创建连接
    public static void init() throws Exception {
        //Class.forName(driverName);
       // conn = DriverManager.getConnection(url, user, password);
        stmt = conn.createStatement();
    }
    //执行语句，返回成功或不成功
    public static boolean execute(String sql)throws Exception {
    	try {
    		init();
			return stmt.execute(sql);
		} catch (Exception e) {
			throw e;
		}finally {
			destory();
		}
    }
    //执行查询返回结果
    public static Map<String,Object> executeQuery(String sql) throws Exception {
    	try {
    		init();
    		Map<String,Object> reMap = new HashMap<String,Object>();
    		rs = stmt.executeQuery(sql);
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int cls = 0;
    		if(rsmd != null) {
    			cls = rsmd.getColumnCount();
    		}
    		List<String> cnameList = new ArrayList<String>();
    		for(int i=0;i<cls;i++) {
    			String cname = rsmd.getColumnName(i+1);
    			cnameList.add(cname);
    		}
    		List<List<Object>> reList = new ArrayList<List<Object>>();
            while (rs.next()) {
            	//Map<String,Object> valMap = new HashMap<String, Object>();
            	List<Object> tlist = new ArrayList<Object>();
            	for(int i=0;i<cls;i++) {
            		Object obj = rs.getObject(i+1);
            		//valMap.put(cname, obj);
            		tlist.add(obj);
            	}
            	reList.add(tlist);
            }
            reMap.put("list",reList);
            reMap.put("cnameList", cnameList);
            return reMap;
		} catch (Exception e) {
			throw e;
		}finally {
			destory();
		}
    }
    
 
    // 创建数据库
    public void createDatabase() throws Exception {
        String sql = "create testdb";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
 
    // 查询所有数据库
    public void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
 
    // 创建表
    public void createTable() throws Exception {
        String sql = "create table emp(\n" +
                "empno int,\n" +
                "ename string,\n" +
                "job string,\n" +
                "mgr int,\n" +
                "hiredate string,\n" +
                "sal double,\n" +
                "comm double,\n" +
                "deptno int\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t'";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
 
    // 查询所有表
    public void showTables() throws Exception {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
 
    // 查看表结构
    public void descTable() throws Exception {
        String sql = "desc emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }
 
    // 加载数据
    public void loadData() throws Exception {
        String filePath = "/home/hadoop/data/emp.txt";
        String sql = "load data local inpath '" + filePath + "' overwrite into table emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
 
    // 查询数据
    public void selectData() throws Exception {
        String sql = "select * from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        System.out.println("员工编号" + "\t" + "员工姓名" + "\t" + "工作岗位");
        while (rs.next()) {
            System.out.println(rs.getString("empno") + "\t\t" + rs.getString("ename") + "\t\t" + rs.getString("job"));
        }
    }
 
    // 统计查询（会运行mapreduce作业）
    public void countData() throws Exception {
        String sql = "select count(1) from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
    }
 
    // 删除数据库
    public void dropDatabase() throws Exception {
        String sql = "drop database if exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
 
    // 删除数据库表
    public void deopTable() throws Exception {
        String sql = "drop table if exists emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
 
    // 释放资源
    public static void destory() throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
		/*
		 * if (conn != null) { //conn.close(); }
		 */
    }
}
