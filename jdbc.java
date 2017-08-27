import java.util.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

class jdbc {
    public static void main(String args[]) {
        String urlPrefix = "jdbc:postgresql://";
        String url       = urlPrefix + "localhost:5432/ubuntu";
        String user      = "ubuntu";
        String password  = "ubuntu";
        Connection con;

        try {
            // Load the jdbc driver
            Class.forName("org.postgresql.Driver");
            System.out.println("***** Loaded the JDBC driver ");

            // Create connection
            con = DriverManager.getConnection(url,user,password);

            // commit changes manually
            con.setAutoCommit(false);
            System.out.println("***** Created a JDBC connection to the data source ");

            // You can execute coding from here
            // ----------------------------------------------------------------------

            String sql = "delete from tb41_a;";
            //execUpdate(con,sql);
            sql = "select * from tb41_a";
            prepareSQL(con);

            // ----------------------------------------------------------------------
            // transaction rollback
            con.rollback();
            System.out.println(" transaction rollback;");
            // transaction commit
            con.commit();
            System.out.println("***** Transaction commited ");

            // Close the connection
            con.close();
            System.out.println("***** Disconnected from data source ");

        } catch (ClassNotFoundException e) {
            System.err.println ("Could not load JDBC driver");
            System.out.println ("Exception: " + e);
            e.printStackTrace();
        } catch (SQLException ex) {
            System.err.println ("SQLException information");
            while (ex!=null) {
                System.err.println ("Error msg: " + ex.getMessage());
                System.err.println ("SQLSTATE:  " + ex.getSQLState());
                System.err.println ("Error code:  " + ex.getErrorCode());
                ex.printStackTrace();
                ex = ex.getNextException();
            }
        }
    }
    private static void execUpdate(Connection con ,String sql) {
        System.out.println("///// Here is execUpdate method");
        Statement stmt;
        System.out.println(sql);
        try {
            stmt = con.createStatement();
            int updateCount = stmt.executeUpdate(sql);
            String msgStr = String.format("----- number of records affected is %d -----",updateCount);
            System.out.println(msgStr);
            if ( stmt != null ) {
                stmt.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }
    private static void execQuery(Connection con , String sql) {
        System.out.println("///// Here is execQuery method");
        Statement stmt;
        ResultSet rs;
        ResultSetMetaData rsmd;
        System.out.println(sql);
        try {
            // Create the Statement
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            System.out.println("***** Created JDBC Statement object ");

            // Execute a query and generate a ResultSet instance
            rs = stmt.executeQuery(sql);
            System.out.println("***** Created JDBC ResultSet object ");

            // Display Resultset
            rsmd = rs.getMetaData();
            List<String> fields = new ArrayList<String>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                fields.add(rsmd.getColumnName(i));
            }
            int columnCount = rsmd.getColumnCount();
            System.out.println(columnCount);
            System.out.println(fields);
            String retval;
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                for (String field : fields) {
                    retval = rs.getString(field);
                    if (rs.wasNull()) {
                        retval = "NULL";
                    }
                    System.out.print(retval);
                    System.out.print(" | ");
                }
                System.out.println("");
            }
            String msgStr = String.format("----- number of selected records is %d -----", rowCount);
            System.out.println(msgStr);

            // Close the ResultSet
            rs.close();
            System.out.println("***** Closed JDBC Resultset ");

            // Close the Statement
            stmt.close();
            System.out.println("***** Closed JDBC Statement ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void prepareSQL( Connection con ) {
        System.out.println("///// Here is prepareSQL method");
        PreparedStatement pstmt;
        ResultSet rs;
        ResultSetMetaData rsmd;
        String sql;
        try {
            // Create the prepared Statement
            sql = 
                "select * " +
                "from p62  " +
                "where name = ? ";
            pstmt = con.prepareStatement(sql);
            System.out.println("***** Created JDBC Statement object ");


            // Execute a query and generate a ResultSet instance
            pstmt.setString(1,"Al");
            rs = pstmt.executeQuery();
            pstmt.clearParameters();
            System.out.println("***** Created JDBC ResultSet object ");

            // Display Resultset
            rsmd = rs.getMetaData();
            List<String> fields = new ArrayList<String>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                fields.add(rsmd.getColumnName(i));
            }
            int columnCount = rsmd.getColumnCount();
            System.out.println(columnCount);
            System.out.println(fields);
            String retval;
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                for (String field : fields) {
                    retval = rs.getString(field);
                    if (rs.wasNull()) {
                        retval = "NULL";
                    }
                    System.out.print(retval);
                    System.out.print(" | ");
                }
                System.out.println("");
            }
            String msgStr = String.format("----- number of selected records is %d -----", rowCount);
            System.out.println(msgStr);

            // Close the ResultSet
            rs.close();
            System.out.println("***** Closed JDBC Resultset ");

            // Close the Statement
            pstmt.close();
            System.out.println("***** Closed JDBC Statement ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
