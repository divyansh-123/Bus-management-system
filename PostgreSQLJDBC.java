import java.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.math.BigDecimal;

import java.util.Scanner;

public class PostgreSQLJDBC 
{
	public static void main(String args[]) 
	{
		Connection c = null;
		try
		{
			// Load Postgresql Driver class
			Class.forName("org.postgresql.Driver");
			//System.out.println("Opened database successfully");
			// Using Driver class connect to databased on localhost, port=5432, database=postgres, user=postgres, password=postgres. If cannot connect then exception will be generated (try-catch block)
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "Bhava@24");
			//c.setAutoCommit(false);
			System.out.println("Opened database successfully");
			
			// Create instance of this class to call other methods
			PostgreSQLJDBC p = new PostgreSQLJDBC();
			// Call method setSearchPath to set path to Bus_database schema
			p.setSearchPath(c);
			
			// Call method findEmployees to Create Run SELECT Query in Employee Table(to display the details of all employees)
			p.findEmployees(c);

			// Call method updseatsBus to Create Run UPDATE Query in Bus table(updates available number of seats)
			p.updseatsBus(c);
			
			//Call method insertStation to Create INSERT date into Station table
			p.insertStation(c);
			
			//Call method deleterecordsemployee to Create DELETE Query to delete details of employee 
			p.deleterecordsemployee(c);
			
			// Call method count_cus to call function (stored procedure) and read return value
			p.count_cust(c);
			
			c.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("Can't open database successfully");
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	
	void setSearchPath(Connection c)
	{
		Statement stmt = null;
		try
		{
			stmt = c.createStatement();
			String sql = "SET search_path TO bus_database;";
			stmt.executeUpdate(sql);
			stmt.close();
			System.out.println("Changed Search Path successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	void findEmployees(Connection c)
	{
		Statement stmt = null;
		try
		{
			stmt = c.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT u.fname AS efname, u.lname AS elname, e.employeeid AS eid,u.userid as uid" + 
			" FROM bus_database.employee AS e NATURAL JOIN bus_database.user_login as u");
			
			while(rs.next())
			{
				
				String efname,elname;
				BigDecimal eid,uid;
				
				eid = rs.getBigDecimal("eid");
				uid = rs.getBigDecimal("uid");
				efname = rs.getString("efname");
				elname = rs.getString("elname");
				
				System.out.println("\nEmployeeInfo: ID-" + eid +", UserID- "+ uid +", Name-" + efname + " " + elname);
			}
			
			stmt.close();
			System.out.println("Table Queried successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
   }
   
   void updseatsBus(Connection c)
	{
		PreparedStatement stmt = null;
		String sql = "UPDATE bus_database.bus SET available_number_of_seats = ? WHERE busid = ?";
		try
		{
			stmt = c.prepareStatement(sql);
			stmt.setBigDecimal(1, new BigDecimal(47)); // updated available number of seats 
			stmt.setBigDecimal(2, new BigDecimal(1534101)); // bus id 
			int affectedRows = stmt.executeUpdate();
			stmt.close();
			System.out.println("\nTable Updated successfully, Rows Updated: " + affectedRows);
		}
		
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	void insertStation(Connection c)
	{
		PreparedStatement stmt = null;
		String sql = "INSERT INTO bus_database.station VALUES (?, ?, ?, ?)";
		try
		{
			Scanner in = new Scanner(System.in);
			stmt = c.prepareStatement(sql);
			
			System.out.println("Enter Station Id: ");
			BigDecimal id = in.nextBigDecimal();
			stmt.setBigDecimal(1,id);

			System.out.println("Enter Station Name: ");
			in.nextLine();
			String s = in.nextLine();
			stmt.setString(2,s);
			
			
			System.out.println("Enter District Name: ");
			s = in.nextLine();
			stmt.setString(3,s);
			
			System.out.println("Enter State Name: ");
			s = in.nextLine();
			stmt.setString(4,s);
		
			int affectedRows = stmt.executeUpdate();
			stmt.close();
			System.out.println("Table Inserted successfully: Rows Affected: " + affectedRows);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	
	void deleterecordsemployee(Connection c)
	{
      Statement stmt = null;
	  try
	  {	
		 stmt = c.createStatement();	      
         String sql = "DELETE FROM User_Login " + "WHERE UserID = 1242121";
		 String QUERY = "SELECT UserID,Username,Email,AadharNo FROM User_Login";
         stmt.executeUpdate(sql);
         ResultSet rs = stmt.executeQuery(QUERY);
         
         while(rs.next()){
            //Display values
            System.out.print("\nUserID: " + rs.getInt("UserID"));
            System.out.print(", Username: " + rs.getString("Username"));
            System.out.print(", Email: " + rs.getString("Email"));
            System.out.println(", Aadhaar number: " + rs.getString("AadharNo"));
         }
         rs.close();
      } 
	  catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
   	}
   	
   	void count_cust(Connection c)
	{
		CallableStatement stmt = null;
		try
		{
			String sql = "{? = CALL tot_customers()}";
			stmt = c.prepareCall(sql);
			stmt.registerOutParameter(1, java.sql.Types.INTEGER);
			stmt.execute();
			int val = stmt.getInt(1);
			
			System.out.println("\nTotal number of customers = " + val); 
			stmt.close();
			System.out.println("Function Called successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
   
}