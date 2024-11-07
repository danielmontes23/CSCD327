import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.String;

public class MyQuery {
    private Connection conn = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    public MyQuery(Connection c) throws SQLException {
        conn = c;
        // Statements allow issuing SQL queries to the database
        statement = conn.createStatement();
    }

    public void findFall2009Students() throws SQLException {
        String query = "SELECT DISTINCT name FROM student NATURAL JOIN takes WHERE semester = 'Fall' AND year = 2009";

        resultSet = statement.executeQuery(query);
    }

    public void printFall2009Students() throws IOException, SQLException {
        System.out.println("******** Query 0 ********");
        System.out.println("Name");
        while (resultSet.next()) {
            String name = resultSet.getString("name");

            System.out.println(name);
        }
    }

    public void findGPAInfo() throws SQLException {
        String Query = "SELECT ID, name, SUM(CASE grade\n" +
                "WHEN 'A' THEN 4.0\n" +
                "WHEN 'A-' THEN 3.7\n" +
                "WHEN 'B+' THEN 3.3\n" +
                "WHEN 'B' THEN 3.0\n" +
                "WHEN 'B-' THEN 2.7\n" +
                "WHEN 'C+' THEN 2.3\n" +
                "WHEN 'C' THEN 2.0\n" +
                "WHEN 'C-' THEN 1.7\n" +
                "WHEN 'D+' THEN 1.3\n" +
                "WHEN 'D' THEN 1.0\n" +
                "WHEN 'D-' THEN 0.7\n" +
                "ELSE 0 \n" +
                "END * CREDITS) / SUM(CREDITS) AS GPA\n" +
                "FROM student NATURAL JOIN takes NATURAL JOIN course\n" +
                "WHERE GRADE IS NOT NULL AND GRADE != 'F'\n" +
                "GROUP BY ID, NAME";

        resultSet = statement.executeQuery(Query);
    }

    public void printGPAInfo() throws IOException, SQLException {
        System.out.println("******** Query 1 ********");
        System.out.printf("%-10s%-10s%-10s%n", "ID", "Name", "GPA");
        while (resultSet.next()) {
            String name = resultSet.getString("name");
            String ID = resultSet.getString("ID");
            double GPA = resultSet.getDouble("GPA");

            System.out.printf("%-10s%-10s%-10.2f%n", ID, name, GPA);
        }
    }

    public void findMorningCourses() throws SQLException {
        String Query =
                "SELECT DISTINCT course.course_id, section.sec_id, course.title, takes.semester, section.year, instructor.name, COUNT(DISTINCT takes.ID) AS enrollment\n" +
                "FROM course\n" +
                "JOIN section ON course.course_id = section.course_id\n" +
                "JOIN time_slot ON section.time_slot_id = time_slot.time_slot_id\n" +
                "JOIN takes ON takes.course_id = section.course_id AND takes.sec_id = section.sec_id AND takes.semester = section.semester AND takes.year = section.year\n" +
                "JOIN teaches ON teaches.course_id = takes.course_id AND takes.sec_id = teaches.sec_id AND takes.semester = teaches.semester AND takes.year = teaches.year\n" +
                "JOIN instructor ON instructor.ID = teaches.ID\n" +
                "WHERE start_hr <= 12\n" +
                "GROUP BY course.course_id, section.sec_id, takes.semester, section.year, instructor.name;";

        resultSet = statement.executeQuery(Query);
    }

    public void printMorningCourses() throws IOException, SQLException {
        System.out.println("******** Query 2 ********");
        System.out.printf("%-10s%-10s%-30s%-10s%-10s%-15s%-10s \n", "Course_ID", "Sec_ID", "Title", "Semester", "Year", "Name", "Enrollment");
        while (resultSet.next()) {
            String course_id = resultSet.getString(1);
            String sec_id = resultSet.getString(2);
            String title = resultSet.getString(3);
            String semester = resultSet.getString(4);
            String year = resultSet.getString(5);
            String name = resultSet.getString(6);
            int enrollment = resultSet.getInt(7);

            System.out.printf("%-10s%-10s%-30s%-10s%-10s%-15s%-10d \n", course_id, sec_id, title, semester, year, name, enrollment);
        }
    }

    public void findBusyClassroom() throws SQLException {
        String Query = "SELECT building, room_number, COUNT(time_slot_id) AS frequency \n" +
                "FROM section \n" +
                "NATURAL JOIN classroom \n" +
                "GROUP BY building, room_number \n" +
                "HAVING COUNT(time_slot_id) >= ALL(SELECT COUNT(time_slot_id) AS frequency \n" +
                "                                   FROM section \n" +
                "                                   NATURAL JOIN classroom \n" +
                "                                   GROUP BY building, room_number);";

        resultSet = statement.executeQuery(Query);
    }

    public void printBusyClassroom() throws IOException, SQLException {
        System.out.println("******** Query 3 ********");
        System.out.printf("%-10s%-10s%-10s \n", "Building", "Room_Number", " Frequency");
        while (resultSet.next()) {
            String building = resultSet.getString(1);
            double room_number = resultSet.getDouble(2);
            int frequency = resultSet.getInt(3);

            System.out.printf("%-10s%-10.0f%-10d \n", building, room_number, frequency);
        }
    }

    public void findPrereq() throws SQLException {
        String Query =
                "SELECT C1.title, IFNULL(C2.title, 'N/A') AS prereq " +
                        "FROM course C1 " +
                        "LEFT JOIN prereq ON C1.course_id = prereq.course_id " +
                        "LEFT JOIN course C2 ON C2.course_id = prereq.prereq_id " +
                        "GROUP BY C1.title, prereq;";

        resultSet = statement.executeQuery(Query);
    }

    public void printPrereq() throws IOException, SQLException {
        System.out.println("******** Query 4 ********");
        System.out.printf("%-30s%-10s \n", "Course", "Pre-req");
        while (resultSet.next()) {
            String course = resultSet.getString("title");
            String prereq = resultSet.getString("prereq");

            System.out.printf("%-30s%-10s \n", course, prereq);
        }
    }
    public void updateTable() throws SQLException {
        String Query =
                "UPDATE studentCopy S " +
                "SET S.tot_cred = COALESCE((SELECT SUM(CASE WHEN T.grade IN ('A+', 'A', 'A-','B+', 'B', 'B-','C+', 'C', 'C-', 'D') THEN C.credits ELSE 0 END) " +
                "                           FROM takes T " +
                "                           JOIN course C ON T.course_id = C.course_id " +
                "                           WHERE T.ID = S.ID " +
                "                           GROUP BY T.ID), 0)";

        statement.executeUpdate(Query);

        String copyTable = "SELECT sC.*, COUNT(DISTINCT T.course_id) AS number_of_Courses " +
                "FROM studentCopy sC " +
                "LEFT JOIN takes T ON sC.ID = T.ID " +
                "GROUP BY sC.ID, sC.name, sC.dept_name, sC.tot_cred";

        resultSet = statement.executeQuery(copyTable);
    }
    public void printUpdatedTable() throws IOException, SQLException {
        System.out.println("******** Query 5 ********");
        System.out.printf("%-10s%-10s%-15s%-10s \n", "ID","Name", "Dept_Name","Tot_cred");
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String dept_name = resultSet.getString("dept_name");
            int tot_cred = resultSet.getInt("tot_cred");
            int num_of_courses = resultSet.getInt("number_of_Courses");

            System.out.printf("%-10s%-10s%-10s     %-10s%-10s \n", id, name, dept_name, tot_cred, num_of_courses);
        }
    }
    public void findDeptInfo() throws SQLException
    {
        System.out.println("******** Query 6 ********");
        Scanner info = new Scanner(System.in);
        System.out.print("Please enter the Department Name: \n");
        String Dept_Name = info.nextLine();

        CallableStatement call =conn.prepareCall("{CALL deptInfo(?, ?, ?, ?)}");
        call.setString(1, Dept_Name);
        call.registerOutParameter(2, Types.INTEGER);
        call.registerOutParameter(3, Types.DECIMAL);
        call.registerOutParameter(4, Types.DECIMAL);
        call.execute();

        int instructors = call.getInt(2);
        int tot_salary = call.getInt(3);
        double budget = call.getDouble(4);

        System.out.print(Dept_Name + " Department has " + instructors + " instructors\n");
        System.out.print(Dept_Name + " Department has a total salary of " + "$"+ tot_salary + ".0" + "\n" );
        System.out.print(Dept_Name + " Department has a budget of " + "$" + budget + "\n");
    }
    public void findFirstLastSemester() throws SQLException {
        String Query =
                "SELECT student.ID, student.name, " +
                        "       CONCAT(CASE " +
                        "               WHEN EXISTS (SELECT 1 FROM takes WHERE semester = 'Spring' AND student.ID = takes.ID AND takes.year = first_sem.year) THEN 'Spring' " +
                        "               WHEN EXISTS (SELECT 1 FROM takes WHERE semester = 'Summer' AND student.ID = takes.ID AND takes.year = first_sem.year) THEN 'Summer' " +
                        "               ELSE 'Fall' " +
                        "              END, ' ', first_sem.year) AS First_Semester, " +
                        "       CONCAT(CASE " +
                        "               WHEN EXISTS (SELECT 1 FROM takes WHERE semester = 'Fall' AND student.ID = takes.ID AND takes.year = last_sem.year) THEN 'Fall' " +
                        "               WHEN EXISTS (SELECT 1 FROM takes WHERE semester = 'Summer' AND student.ID = takes.ID AND takes.year = last_sem.year) THEN 'Summer' " +
                        "               ELSE 'Spring' " +
                        "              END, ' ', last_sem.year) AS Last_Semester " +
                        "FROM student " +
                        "JOIN (SELECT ID, MIN(year) AS year FROM takes GROUP BY ID) AS first_sem ON student.ID = first_sem.ID " +
                        "JOIN (SELECT ID, MAX(year) AS year FROM takes GROUP BY ID) AS last_sem ON student.ID = last_sem.ID " +
                        "WHERE EXISTS (SELECT 1 FROM takes WHERE student.ID = takes.ID);";

        resultSet = statement.executeQuery(Query);
    }

    public void printFirstLastSemester() throws IOException, SQLException {
        System.out.println("******** Query 7 ********");
        System.out.printf("%-10s%-10s%-18s%-10s \n", "ID","Name", "First Semester","Last Semester");
        while (resultSet.next()) {
            int ID = resultSet.getInt(1);
            String Name = resultSet.getString(2);
            String First_Semester = resultSet.getString(3);
            String Last_Semester = resultSet.getString(4);

            System.out.printf("%-10d%-10s%-18s%-10s \n", ID, Name, First_Semester, Last_Semester);
        }
    }
}


