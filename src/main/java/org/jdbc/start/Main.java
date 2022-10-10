package org.jdbc.start;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;

public class Main {

    /**
     * JDBC Driver and database url
     */
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";//"com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static final String DATABASE_URL = "jdbc:mysql://localhost:3306/it-academy";//"jdbc:sqlserver://localhost:1433;DatabaseName=it-academy;integratedSecurity=true;"; //"jdbc:h2:tcp://localhost/~/test";

    /**
     * User and Password
     */

    static final String SQL = "SELECT * FROM user";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        first();
        //navigationByResultSet();
        //increaseResultSet();
        //dataTypesConversionDemo();
        //savepointDemo();
        //jdbcExceptionDemo();
        //batchingProcessingDemo();
        //streamingDataDemo();
    }

    public static void first() throws ClassNotFoundException, SQLException {
        Connection connection = null;
        Statement statement = null;

        System.out.println("Registering JDBC driver...");

       // Class.forName(JDBC_DRIVER);
        Driver driver = new com.mysql.cj.jdbc.Driver();
        DriverManager.registerDriver(driver);

        System.out.println("Creating database connection...");
        connection = DriverManager.getConnection(DATABASE_URL, "root", "root");
       // connection.setAutoCommit(false);
        System.out.println("Executing statement...");
        statement = connection.createStatement();

      /*  String SQL_success = "INSERT INTO user(name, email, password, role)\n" +
                "\tVALUES('user3', 'user3@tut.by', 'Password3@', 'user')";
        statement.executeUpdate(SQL_success);
        connection.commit();*/

        ResultSet resultSet = statement.executeQuery(SQL);

        System.out.println("Retrieving data from database...");
        System.out.println("\nDevelopers:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String email = resultSet.getString("email");
            String password = resultSet.getString("password");

            System.out.println("\n================\n");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + email);
            System.out.println("password: " + password);
        }

        System.out.println("Closing connection and releasing resources...");
        resultSet.close();
        statement.close();
        connection.close();
    }

    public static void navigationByResultSet() throws SQLException, ClassNotFoundException {

        Connection connection = null;
        Statement statement = null;

        Class.forName(JDBC_DRIVER);

        connection = DriverManager.getConnection(DATABASE_URL, "root", "root");

        System.out.println("Creating statement...");

        try {
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = statement.executeQuery(SQL);

            System.out.println("Moving cursor to the last position...");
            resultSet.last();

            System.out.println("Getting record...");
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String specialty = resultSet.getString("specialty");
            int salary = resultSet.getInt("salary");

            System.out.println("Last record in result set:");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
            System.out.println("\n=========================\n");

            System.out.println("Moving cursor to previous row...");
            resultSet.previous();

            System.out.println("Getting record (by name)...");
            id = resultSet.getInt("id");
            name = resultSet.getString("name");
            specialty = resultSet.getString("specialty");
            salary = resultSet.getInt("salary");

            System.out.println("Previous record:");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
            System.out.println("\n=========================\n");

            System.out.println("Moving cursor to the first row...");
            resultSet.first();

            System.out.println("Getting record (by name)...");
            id = resultSet.getInt("id");
            name = resultSet.getString("name");
            specialty = resultSet.getString("specialty");
            salary = resultSet.getInt("salary");

            System.out.println("First record:");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
            System.out.println("\n=========================\n");

            System.out.println("Adding record...");
            statement.executeUpdate("INSERT INTO dbo.developers VALUES ('Mike', 'PHP', 1500)");

            resultSet = statement.executeQuery(SQL);
            resultSet.last();

            System.out.println("Getting record (by index)...");
            id = resultSet.getInt(1);
            name = resultSet.getString(2);
            specialty = resultSet.getString(3);
            salary = resultSet.getInt(4);

            System.out.println("Last record:");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
            System.out.println("\n=========================\n");


            System.out.println("Full list of records:");
            resultSet = statement.executeQuery(SQL);

            while (resultSet.next()) {
                id = resultSet.getInt("id");
                name = resultSet.getString("name");
                specialty = resultSet.getString("specialty");
                salary = resultSet.getInt("salary");

                System.out.println("id: " + id);
                System.out.println("Name: " + name);
                System.out.println("Specialty: " + specialty);
                System.out.println("Salary: $" + salary);
                System.out.println("\n=========================\n");

            }


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void increaseResultSet() throws SQLException, ClassNotFoundException {

        Connection connection = null;
        Statement statement = null;

        Class.forName(JDBC_DRIVER);

        connection = DriverManager.getConnection(DATABASE_URL);

        System.out.println("Creating statement...");

        try {
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE
            );
            ResultSet resultSet = statement.executeQuery(SQL);

            System.out.println("Initial list of records:");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String specialty = resultSet.getString("specialty");
                int salary = resultSet.getInt("salary");

                System.out.println("Record in result set:");
                System.out.println("id: " + id);
                System.out.println("Name: " + name);
                System.out.println("Specialty: " + specialty);
                System.out.println("Salary: $" + salary);
                System.out.println("\n=========================\n");
            }

            System.out.println("Increasing all developer's salary (+ $1,000)...");
            resultSet.first();
            while (resultSet.next()) {
                int newSalary = resultSet.getInt("salary") + 1000;
                resultSet.updateInt("salary", newSalary);
                resultSet.updateRow();
            }

            resultSet = statement.executeQuery(SQL);
            System.out.println("Final list of records:");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String specialty = resultSet.getString("specialty");
                int salary = resultSet.getInt("salary");

                System.out.println("id: " + id);
                System.out.println("Name: " + name);
                System.out.println("Specialty: " + specialty);
                System.out.println("Salary: $" + salary);
                System.out.println("\n=========================\n");
            }
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void dataTypesConversionDemo() {
        /**
         *  Java Date and Time
         */
        java.util.Date javaDate = new java.util.Date();
        long javaTime = javaDate.getTime();

        System.out.println("Current date and time (Java): " + javaDate.toString());
        System.out.printf("Current time (Java): " + javaDate.getHours() + ":" +
                javaDate.getMinutes() + ":"
                + javaDate.getSeconds());

        /**
         * SQL Date and Time
         */
        java.sql.Timestamp sqlTimeStamp = new java.sql.Timestamp(javaTime);
        java.sql.Date sqlDate = new java.sql.Date(javaTime);
        java.sql.Time sqlTime = new java.sql.Time(javaTime);

        System.out.println("\n=======================================\n");
        System.out.println("Current timeStamp (SQL): " + sqlTimeStamp.toString());
        System.out.println("Current date (SQL): " + sqlDate.toString());
        System.out.println("Current time (SQL): " + sqlTime.toString());

    }

    public static void savepointDemo() throws ClassNotFoundException, SQLException {
        Connection connection = null;
        Statement statement = null;

        Class.forName(JDBC_DRIVER);

        connection = DriverManager.getConnection(DATABASE_URL);
        connection.setAutoCommit(false);

        statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery(SQL);

        System.out.println("Retrieving data from database...");
        System.out.println("\ndevelopers:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String specialty = resultSet.getString("specialty");
            int salary = resultSet.getInt("salary");

            System.out.println("\n================\n");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
        }

        System.out.println("\n===========================\n");
        System.out.println("Creating savepoint...");
        Savepoint savepointOne = connection.setSavepoint("SavepointOne");

        try {
            String SQL_failed = "INSERT INTO dbo.developer VALUES ('John','C#', 2200)";
            statement.executeUpdate(SQL_failed);

            String SQL_success = "INSERT INTO dbo.developers VALUES ('Ron', 'Ruby', 1900)";
            statement.executeUpdate(SQL_success);

            connection.commit();
        } catch (SQLException e) {
            System.out.println("SQLException. Executing rollback to savepoint...");
            connection.rollback(savepointOne);
        }
        resultSet = statement.executeQuery(SQL);
        System.out.println("Retrieving data from database...");
        System.out.println("\ndevelopers:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String specialty = resultSet.getString("specialty");
            int salary = resultSet.getInt("salary");

            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary: $" + salary);
            System.out.println("\n================\n");
        }

        System.out.println("Closing connection and releasing resources...");
        resultSet.close();
        statement.close();
        connection.close();
    }

    public static void jdbcExceptionDemo() {
        Connection connection = null;

        try {
            Class.forName(JDBC_DRIVER);

            System.out.println("Connecting to database...");
            connection = DriverManager.getConnection(DATABASE_URL);

            System.out.println("Creating statement...");
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(SQL);

            System.out.println("Displaying retrieved records...");

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String specialty = resultSet.getString("specialty");
                int salary = resultSet.getInt("salary");

                System.out.println("\n==============");
                System.out.println("id: " + id);
                System.out.println("Name: " + name);
                System.out.println("Specialty: " + specialty);
                System.out.println("Salary: " + salary);
                System.out.println("==============\n");
            }

            System.out.println("Releasing resources...");
            resultSet.close();
            statement.close();
            connection.close();


            System.out.println("Thank You.");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void batchingProcessingDemo() throws ClassNotFoundException, SQLException {
        Connection connection = null;
        Statement statement = null;
        String SQL_INSERT = "";

        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(DATABASE_URL);
        connection.setAutoCommit(false);

        statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery(SQL);


        System.out.println("Initial developer's table content:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");

            if (name == null) {
                System.out.println("Table is empty.");
                break;
            }
            String specialty = resultSet.getString("specialty");
            int salary = resultSet.getInt("salary");

            System.out.println("\n====================");
            System.out.println("id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Specialty: " + specialty);
            System.out.println("Salary : $" + salary);
            System.out.println("====================\n");
        }

        SQL_INSERT = "INSERT INTO dbo.developers VALUES ('Proselyte', 'Java', 3000)";
        statement.addBatch(SQL_INSERT);
        SQL_INSERT = "INSERT INTO dbo.developers VALUES ('AsyaSmile', 'UI/UX', 2500)";
        statement.addBatch(SQL_INSERT);
        SQL_INSERT = "INSERT INTO dbo.developers VALUES ('Peter', 'C++', 3000)";
        statement.addBatch(SQL_INSERT);

        try {

            statement.executeBatch();
            connection.commit();

            resultSet = statement.executeQuery(SQL);

            System.out.println("Final developer's table content:");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String specialty = resultSet.getString("specialty");
                int salary = resultSet.getInt("salary");

                System.out.println("\n====================");
                System.out.println("id: " + id);
                System.out.println("Name: " + name);
                System.out.println("Specialty: " + specialty);
                System.out.println("Salary : $" + salary);
                System.out.println("====================\n");
            }

            resultSet.close();
            statement.close();
            connection.close();
        } finally {
            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                connection.close();
            }
        }

        System.out.println("Thank You.");
    }

    public static void streamingDataDemo() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DATABASE_URL);

            stmt = conn.createStatement();

            File f = new File("ProselyteDeveloper.xml");
            long fileLength = f.length();
            FileInputStream fis = new FileInputStream(f);

            String SQL = "INSERT INTO dbo.XML_Developer VALUES (?)";
            pstmt = conn.prepareStatement(SQL);
            pstmt.setAsciiStream(1, fis, (int) fileLength);
            pstmt.execute();

            fis.close();

            SQL = "SELECT Data FROM dbo.XML_Developer WHERE id=1";
            rs = stmt.executeQuery(SQL);
            if (rs.next()) {
                InputStream xmlInputStream = rs.getAsciiStream(1);
                int c;
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                while ((c = xmlInputStream.read()) != -1)
                    bos.write(c);
                System.out.println(bos.toString());
            }
            rs.close();
            stmt.close();
            pstmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("Thank You!");
    }

}

