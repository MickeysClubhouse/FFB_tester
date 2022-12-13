import java.sql.*;

public class Client{
    private Statement stmt;


    public Client(String host, String db, String port, String username, String password){
        try {
            Class.forName(("com.mysql.cj.jdbc.Driver"));
            String url="jdbc:mysql://"+host+":"+port+"/"+db+"?serverTimezone=GMT";

            Connection conn=DriverManager.getConnection(url,username,password);
//            if(conn!=null){
//                System.out.print("连接成功");
//            }


            this.stmt = conn.createStatement();
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
                throw new RuntimeException(e);
            }
        }


        return rs;

    }


}