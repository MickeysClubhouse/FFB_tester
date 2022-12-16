import java.sql.*;
import java.util.ResourceBundle;

public class Client{
    private Statement stmt;
    private Connection conn;
    private int timeout=20;


    public Client(){
        ResourceBundle configs = ResourceBundle.getBundle("test_config");
        try {
            Class.forName(("com.mysql.cj.jdbc.Driver"));
            String url="jdbc:mysql://"+configs.getString("host")+":"+configs.getString("port")+"/"+configs.getString("db")+"?serverTimezone=GMT";

            this.conn=DriverManager.getConnection(url,configs.getString("username"),configs.getString("password"));
            this.stmt = conn.createStatement();
            this.timeout=Integer.parseInt(configs.getString("timeout"));
            this.stmt.setQueryTimeout(this.timeout);
            /*
             * Statement有三种执行SQL的方法
             * 1.execute()可执行任何SQL语句返回boolean
             * 2.executeQuery()返回ResultSet
             * 3.executeUpdate()执行DML语句返回受影响记录数
             */

        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean execSQL(String SQL){
        boolean rs=false;

        if(!SQL.equals("")){
            try {
                rs = stmt.execute(SQL);
                //ResultSet通过next()能向前迭代，通过各种getXxx()方法获取对应字段值
            } catch (Exception e) {
                System.out.println("[SLOW SQL]exceeding time limit:"+timeout+"s");
            }
        }
        return rs;
    }

    public void release(){
        try {
            this.stmt.close();
            this.conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}