/* 
This is a Java skeleton code to help you out with how to start this assignment.
Please remember that this is NOT a compilable/runnable java file.
Please feel free to use this skeleton code.
Please look closely at the "To Do" parts of this file. You may get an idea of how to finish this assignment. 
*/

//ssh -N -p922 -L4321:mysql.cs.wwu.edu:3306 hughesw@proxy.cs.wwu.edu

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class Assign2Skeleton {
	
	static class StockData {	   
		// To Do: 
		// Create this class which should contain the information  (date, open price, high price, low price, close price) for a particular ticker
	}
	
	static Connection conn;
	static final String prompt = "Enter ticker symbol [start/end dates]: ";
	
	public static void main(String[] args) throws Exception {
		//String paramsFile = "ConnectionParameters_LabComputer.txt";
		String paramsFile = "ConnectionParameters_RemoteComputer.txt";

		if (args.length >= 1) {
			paramsFile = args[0];
		}
		
		Properties connectprops = new Properties();
		connectprops.load(new FileInputStream(paramsFile));
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String dburl = connectprops.getProperty("dburl");
			String username = connectprops.getProperty("user");
			conn = DriverManager.getConnection(dburl, connectprops);
			System.out.println("Database connection is established");
			
			Scanner in = new Scanner(System.in);
			System.out.print(prompt);
			String input = in.nextLine().trim();
			
			while (input.length() > 0) {
				String[] params = input.split("\\s+");
				String ticker = params[0];
				String startdate = null, enddate = null;
				if (getName(ticker)) {
					if (params.length >= 3) {
						startdate = params[1];
						enddate = params[2];
					}               
					Deque<StockData> data = getStockData(ticker, startdate, enddate);
					System.out.println();
					System.out.println("Executing investment strategy");
					doStrategy(ticker, data);
				} 
				
				System.out.println();
				System.out.print(prompt);
				input = in.nextLine().trim();
			}

			// Close the database connection

		} catch (SQLException ex) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
							ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		}
	}
	
	// Execute the first query and print the company name of the ticker user provided (e.g., INTC to Intel Corp.)
	static boolean getName(String ticker) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(
                "select Name " +
                "  from company " +
                "  where Ticker = ?");

        pstmt.setString(1, ticker);
        ResultSet rs = pstmt.executeQuery();

        if (rs.next()) {
            System.out.printf("%s%n", rs.getString(1).trim());
        } else {
            System.out.printf("Stock ticker %s is not in the database.%n", ticker);
			return false;
        }
        pstmt.close();
		return true;
	}

	static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException{	  
		// To Do: 
		// Execute the second query, which will return stock information of the ticker (descending on the transaction date)
		// Please don't forget to use a prepared statement	   
		PreparedStatement pstmt = conn.prepareStatement(
                "select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
                "  from pricevolume " +
                "  where Ticker = ? and TransDate >= '2020.11.20' and TransDate <= '2024.11.23'" +
				" order by TransDate DESC");

        pstmt.setString(1, ticker);
        ResultSet rs = pstmt.executeQuery();


        while (rs.next()) {
            System.out.println("TransDate: " + rs.getString(1).trim() +
                ", Open: " + Double.parseDouble(rs.getString(2).trim()) +
                ", High: " + Double.parseDouble(rs.getString(3).trim()) +
                ", Low: " + Double.parseDouble(rs.getString(4).trim()) +
                ", Close: " + Double.parseDouble(rs.getString(5).trim()) +
                ", Volume: " + Double.parseDouble(rs.getString(6).trim()) +
                ", AdjustedClose: " + Double.parseDouble(rs.getString(7).trim()));
		}
        pstmt.close();


		Deque<StockData> result = new ArrayDeque<>();

		// To Do: 
		// Loop through all the dates of that company (descending order)
		// Find a split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
		// Include the adjusted data to the result (which is a Deque); You can use addFirst method for that purpose
				
		return result;
	}
	
	static void doStrategy(String ticker, Deque<StockData> data) {
		//To Do: 
		// Apply Steps 2.6 to 2.10 explained in the assignment description 
		// data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
	}
	}