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
		private String transDate;
		private double openPrice, highPrice, lowPrice, closePrice, volume, adjustedClose;

		public StockData(String transDate, double openPrice, double highPrice, double lowPrice,
						double closePrice, double volume, double adjustedClose) {
			this.transDate = transDate;
			this.openPrice = openPrice;
			this.highPrice = highPrice;
			this.lowPrice = lowPrice;
			this.closePrice = closePrice;
			this.volume = volume;
			this.adjustedClose = adjustedClose;
		}

		// Getters and Setters (if needed)
		public String getTransDate() { return transDate; }
		public void setTransDate(String transDate) { this.transDate = transDate; }

		public double getOpenPrice() { return openPrice; }
		public void setOpenPrice(double openPrice) { this.openPrice = openPrice; }

		public double getHighPrice() { return highPrice; }
		public void setHighPrice(double highPrice) { this.highPrice = highPrice; }

		public double getLowPrice() { return lowPrice; }
		public void setLowPrice(double lowPrice) { this.lowPrice = lowPrice; }

		public double getClosePrice() { return closePrice; }
		public void setClosePrice(double closePrice) { this.closePrice = closePrice; }

		public double getVolume() { return volume; }
		public void setVolume(double volume) { this.volume = volume; }

		public double getAdjustedClose() { return adjustedClose; }
		public void setAdjustedClose(double adjustedClose) { this.adjustedClose = adjustedClose; }

		@Override
		public String toString() {
			return "date: " + transDate +
					", open: " + openPrice +
					", high: " + highPrice +
					", low: " + lowPrice +
					", close: " + closePrice +
					", volume: " + volume +
					", adjusted close: " + adjustedClose;
		}
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
					//System.out.println(data);
					System.out.println(data.getFirst().toString());
					System.out.println(data.getLast().toString());
					
					System.out.println("\nExecuting investment strategy");
					doStrategy(ticker, data);
				} 
				
				System.out.println();
				System.out.print(prompt);
				input = in.nextLine().trim();
			}

			conn.close();

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
				"  where Ticker = ?"
		);

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
		Deque<StockData> result = new ArrayDeque<>();
		
		if (start == null || end == null) { // no range of dates? show all data
			PreparedStatement pstmt = conn.prepareStatement(
					"select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
					"  from pricevolume " +
					"  where Ticker = ?" +
					" order by TransDate DESC"
			);
			pstmt.setString(1, ticker);
			ResultSet rs = pstmt.executeQuery();

			double closingPrice = 0;
			double openingPrice = 0;
			StockData currDay = null;
			StockData prevDay = null;
			int numOfSplits = 0;
			double divisor = 1;

			if (rs.next()) {
				prevDay = new StockData(
						rs.getString(1).trim(),
						Double.parseDouble(rs.getString(2).trim()),
						Double.parseDouble(rs.getString(3).trim()),
						Double.parseDouble(rs.getString(4).trim()),
						Double.parseDouble(rs.getString(5).trim()),
						Double.parseDouble(rs.getString(6).trim()),
						Double.parseDouble(rs.getString(7).trim())
				);
			}

			while (rs.next()) {
				currDay = new StockData(
						rs.getString(1).trim(),
						Double.parseDouble(rs.getString(2).trim()),
						Double.parseDouble(rs.getString(3).trim()),
						Double.parseDouble(rs.getString(4).trim()),
						Double.parseDouble(rs.getString(5).trim()),
						Double.parseDouble(rs.getString(6).trim()),
						Double.parseDouble(rs.getString(7).trim())
				);

				double split = check_lines_for_split(prevDay, currDay);
				divisor = divisor * split;
				if (split != 1) { // stock split occured
					numOfSplits++;
				}

				result.addFirst(prevDay);
				prevDay = currDay;

			}
			result.addFirst(prevDay);
			System.out.println(numOfSplits + ", divisor: " + divisor);
			pstmt.close();
		} else {
			PreparedStatement pstmt = conn.prepareStatement(
					"select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
					" from pricevolume " +
					" where Ticker = ?" +
					" and TransDate BETWEEN ? AND ? " +
					" order by TransDate DESC"
			);

			pstmt.setString(1, ticker);
			pstmt.setString(2, start);
			pstmt.setString(3, end);
			ResultSet rs = pstmt.executeQuery();

			//INTC 1997.07.05 1997.07.17
			StockData currDay = null;
			StockData prevDay = null;
			int numOfSplits = 0;
			double divisor = 1;
			boolean splitToday = false;

			if (rs.next()) {
				System.out.println("D: " + rs.getString(1).trim() +
						", O: " + Double.parseDouble(rs.getString(2).trim()) +
						", H: " + Double.parseDouble(rs.getString(3).trim()) +
						", L: " + Double.parseDouble(rs.getString(4).trim()) +
						", C: " + Double.parseDouble(rs.getString(5).trim()) +
						", V: " + Double.parseDouble(rs.getString(6).trim()) +
						", AC: " + Double.parseDouble(rs.getString(7).trim())
				);
				prevDay = new StockData(
						rs.getString(1).trim(),
						Double.parseDouble(rs.getString(2).trim()),
						Double.parseDouble(rs.getString(3).trim()),
						Double.parseDouble(rs.getString(4).trim()),
						Double.parseDouble(rs.getString(5).trim()),
						Double.parseDouble(rs.getString(6).trim()),
						Double.parseDouble(rs.getString(7).trim())
				);
			}

			while (rs.next()) {
				// System.out.println("D: " + rs.getString(1).trim() +
				// 		", O: " + Double.parseDouble(rs.getString(2).trim()) +
				// 		", H: " + Double.parseDouble(rs.getString(3).trim()) +
				// 		", L: " + Double.parseDouble(rs.getString(4).trim()) +
				// 		", C: " + Double.parseDouble(rs.getString(5).trim()) +
				// 		", V: " + Double.parseDouble(rs.getString(6).trim()) +
				// 		", AC: " + Double.parseDouble(rs.getString(7).trim())
				// );

				currDay = new StockData(
						rs.getString(1).trim(),
						Double.parseDouble(rs.getString(2).trim()),
						Double.parseDouble(rs.getString(3).trim()),
						Double.parseDouble(rs.getString(4).trim()),
						Double.parseDouble(rs.getString(5).trim()),
						Double.parseDouble(rs.getString(6).trim()),
						Double.parseDouble(rs.getString(7).trim())
				);

				double split = check_lines_for_split(prevDay, currDay);
				divisor = divisor * split;
				if (split != 1) { // stock split occured
					numOfSplits++;
					splitToday = true;
				}
				
				if (!splitToday) {
					prevDay.setOpenPrice(Math.round((prevDay.getOpenPrice() / divisor) * 100.0) / 100.0);
					prevDay.setClosePrice(Math.round((prevDay.getClosePrice() / divisor) * 100.0) / 100.0);
					prevDay.setHighPrice(Math.round((prevDay.getHighPrice() / divisor) * 100.0) / 100.0);
					prevDay.setLowPrice(Math.round((prevDay.getLowPrice() / divisor) * 100.0) / 100.0);
				}

				result.addFirst(prevDay);
				//System.out.println(result.getFirst() + "\n");
				prevDay = currDay;
				splitToday = false;
			}

			if (!splitToday) {
				prevDay.setOpenPrice(Math.round((prevDay.getOpenPrice() / divisor) * 100.0) / 100.0);
				prevDay.setClosePrice(Math.round((prevDay.getClosePrice() / divisor) * 100.0) / 100.0);
				prevDay.setHighPrice(Math.round((prevDay.getHighPrice() / divisor) * 100.0) / 100.0);
				prevDay.setLowPrice(Math.round((prevDay.getLowPrice() / divisor) * 100.0) / 100.0);
			}
			result.addFirst(prevDay);
			System.out.println(numOfSplits + ", divisor: " + divisor);
			//System.out.println(result);
			pstmt.close();
		}

		// To Do: 
		// Loop through all the dates of that company (descending order)
		// Find a split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
		// Include the adjusted data to the result (which is a Deque); You can use addFirst method for that purpose
				
		return result;
	}

	/*
		Takes the 2 observed lines of data and checks if the stock split occurred.
		Also makes sure the two lines have the same ticker symbol.
	*/
	public static double check_lines_for_split (StockData prev, StockData curr) {
		//System.out.println(prev.transDate + " closing: " + prev.closePrice + ", " + curr.transDate + " opening: " + curr.openPrice + "\n");
		if (Math.abs(curr.closePrice / prev.openPrice - 1.5) < 0.15) {
			System.out.printf("3:2 split on %s: %.2f --> %.2f%n",
				curr.transDate, curr.closePrice, prev.openPrice);
			return 1.5;
		}
		if (Math.abs(curr.closePrice / prev.openPrice - 2.0) < 0.20) {
			System.out.printf("2:1 split on %s: %.2f --> %.2f%n",
				curr.transDate, curr.closePrice, prev.openPrice);
			return 2;
		}
		if (Math.abs(curr.closePrice / prev.openPrice - 3.0) < 0.30) {
			System.out.printf("3:1 split on %s: %.2f --> %.2f%n",
				curr.transDate, curr.closePrice, prev.openPrice);
			return 3;
		}
		return 1;
	}
	
	static void doStrategy(String ticker, Deque<StockData> data) {
		//To Do: 
		// Apply Steps 2.6 to 2.10 explained in the assignment description 
		// data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
	}
}



/*
Enter ticker symbol [start/end dates]: INTC 1997.07.05 1997.07.17
Intel Corp.
D: 1997.07.17, O: 88.25, H: 89.69, L: 86.12, C: 87.81, V: 1.029464E8, AC: 16.3
D: 1997.07.16, O: 86.38, H: 88.88, L: 84.75, C: 88.38, V: 1.888276E8, AC: 16.4        
D: 1997.07.15, O: 80.75, H: 81.94, L: 79.69, C: 80.91, V: 1.376504E8, AC: 15.01       
D: 1997.07.14, O: 77.25, H: 78.98, L: 76.75, C: 78.75, V: 9.10148E7, AC: 14.61        
D: 1997.07.11, O: 150.5, H: 154.0, L: 149.88, C: 153.81, V: 8.86416E7, AC: 14.27      
D: 1997.07.10, O: 152.5, H: 153.5, L: 149.75, C: 150.12, V: 1.046712E8, AC: 13.93     
D: 1997.07.09, O: 151.0, H: 154.56, L: 151.0, C: 152.62, V: 1.344272E8, AC: 14.16     
D: 1997.07.08, O: 147.81, H: 150.38, L: 146.25, C: 149.62, V: 8.86408E7, AC: 13.88    
D: 1997.07.07, O: 145.69, H: 149.0, L: 145.38, C: 147.38, V: 8.10528E7, AC: 13.67 
*/
