//ssh -N -p922 -L4321:mysql.cs.wwu.edu:3306 hughesw@proxy.cs.wwu.edu
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class HughesAssignment2 {
	static BufferedWriter writer;
	
	static class StockData {
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
		String logFile = "logging_file.txt";

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
			
			writer = new BufferedWriter(new FileWriter(logFile));
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
					System.out.println("\nExecuting investment strategy");
					doStrategy(ticker, data);
				} 
				
				System.out.println();
				System.out.print(prompt);
				input = in.nextLine().trim();
			}
			writer.close();
			in.close();
			conn.close();
			System.out.println("Database connection closed.");

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
            System.out.printf("%s not found in database.%n", ticker);
			return false;
        }
        pstmt.close();
		return true;
	}

	static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException{		
		Deque<StockData> result = new ArrayDeque<>();
		StockData nextDay = null;
		StockData currDay = null;
		int numOfSplits = 0;
		int numOfDays = 1;
		double divisor = 1;
		boolean splitToday = false;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		if (start == null || end == null) { // no range of dates? show all data
			pstmt = conn.prepareStatement(
					"select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
					"  from pricevolume " +
					"  where Ticker = ?" +
					" order by TransDate DESC"
			);
			pstmt.setString(1, ticker);
			rs = pstmt.executeQuery();
		} else {
			pstmt = conn.prepareStatement(
					"select TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose " +
					" from pricevolume " +
					" where Ticker = ?" +
					" and TransDate BETWEEN ? AND ? " +
					" order by TransDate DESC"
			);

			pstmt.setString(1, ticker);
			pstmt.setString(2, start);
			pstmt.setString(3, end);
			rs = pstmt.executeQuery();
		}

		if (rs.next()) { // initialize first day for keeping track of 2 stock days at once
			// useful for debugging return of sql queries
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
			nextDay = new StockData(
					rs.getString(1).trim(),
					Double.parseDouble(rs.getString(2).trim()),
					Double.parseDouble(rs.getString(3).trim()),
					Double.parseDouble(rs.getString(4).trim()),
					Double.parseDouble(rs.getString(5).trim()),
					Double.parseDouble(rs.getString(6).trim()),
					Double.parseDouble(rs.getString(7).trim())
			);
			//adjust stock data based on previous splits
			StockData temp = new StockData(
					currDay.transDate,
					(currDay.getOpenPrice() / divisor), 
					(currDay.getHighPrice() / divisor), 
					(currDay.getLowPrice() / divisor), 
					(currDay.getClosePrice() / divisor), 
					currDay.volume, 
					currDay.adjustedClose);

			double split = checkLinesForSplit(currDay, nextDay);
			divisor = divisor * split;
			if (split != 1) { // stock split occured
				numOfSplits++;
				splitToday = true; // flag buy for next day
			}

			result.addFirst(temp);
			currDay = nextDay; // 
			splitToday = false;
			numOfDays++;
		}

		// add last remaining stock day
		StockData temp = new StockData(
					currDay.transDate,
					(currDay.getOpenPrice() / divisor), 
					(currDay.getHighPrice() / divisor), 
					(currDay.getLowPrice() / divisor), 
					(currDay.getClosePrice() / divisor), 
					currDay.volume, 
					currDay.adjustedClose);
		result.addFirst(temp);

		System.out.println(numOfSplits + " splits in " + numOfDays + " trading days");
		pstmt.close();		
		return result;
	}

	//Takes the 2 observed lines of data and checks if the stock split occurred.
	public static double checkLinesForSplit (StockData curr, StockData next) {
		if (Math.abs(next.closePrice / curr.openPrice - 1.5) < 0.15) {
			System.out.printf("3:2 split on %s: %.2f --> %.2f%n",
				next.transDate, next.closePrice, curr.openPrice);
			return 1.5;
		}
		if (Math.abs(next.closePrice / curr.openPrice - 2.0) < 0.20) {
			System.out.printf("2:1 split on %s: %.2f --> %.2f%n",
				next.transDate, next.closePrice, curr.openPrice);
			return 2;
		}
		if (Math.abs(next.closePrice / curr.openPrice - 3.0) < 0.30) {
			System.out.printf("3:1 split on %s: %.2f --> %.2f%n",
				next.transDate, next.closePrice, curr.openPrice);
			return 3;
		}
		return 1;
	}
	
	static void doStrategy(String ticker, Deque<StockData> data) {
		//To Do: 
		// Apply Steps 2.6 to 2.10 explained in the assignment description 
		// data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
		Deque<Double> movingAverage = new ArrayDeque<>();
		if (data.size() < 51) {
			System.out.println("not enough trading days to execute strategy\nNet cash = 0");
			return;
		}

		int count = 0;
		double avg = 0;
		double cash = 0;
		int shares = 0;
		boolean buyFlag = false;
		double yesterdayClose = 0;
		StockData today = null;
		int transCount = 0;
		while (data.size() > 1) { //go to second to last trading day
			today = data.poll();

			if (count < 50) { //populate 50 day moving average
				movingAverage.addFirst(today.closePrice);
				logMessage(String.format("%s open: %f high: %f low: %f close: %f%n", 
						today.transDate, today.openPrice, today.highPrice, today.lowPrice, today.closePrice));
			} else {
				avg = findFiftyDayAverage(movingAverage);
				movingAverage.addFirst(today.closePrice);
				movingAverage.removeLast();
				logMessage(String.format("%s open: %f high: %f low: %f close: %f (average %f)%n", 
						today.transDate, today.openPrice, today.highPrice, today.lowPrice, today.closePrice, avg));
				if (buyFlag) {
					shares = shares + 100;
					cash = cash - (today.openPrice * 100) - 8;
					transCount++;
					logMessage(String.format("Buy: %s 100 shares @ %f, total shares = %d, cash = %f%n", 
											today.transDate, today.openPrice, shares, cash));
				}
				buyFlag = false;
				if (today.closePrice < avg && (today.closePrice / today.openPrice < 0.97000001)) { // buy criterion
					buyFlag = true;
				} else if (shares >= 100 && today.openPrice > avg && (today.openPrice / yesterdayClose > 1.00999999)) { // sell criterion
					shares = shares - 100;
					cash = cash + (100 * ((today.openPrice + today.closePrice)/2)) - 8;
					transCount++;
					logMessage(String.format("Sell: %s 100 shares @ %f, total shares = %d, cash = %f%n", 
											today.transDate, ((today.openPrice + today.closePrice)/2), shares, cash));
				}
			}
			yesterdayClose = today.closePrice;
			count++;
		}

		avg = findFiftyDayAverage(movingAverage);
		today = data.poll();
		if (shares > 0) {
			cash = cash + (shares * today.openPrice);
			shares = 0;
			transCount++;
		}
		logMessage(String.format("Final sale: %s %d shares @ %f, cash = %f (average = %f)%n", 
								today.transDate, shares, today.openPrice, cash, avg));
		System.out.printf("Transactions executed: %d%nNet cash: %.2f%n", transCount, cash);
	}

	public static double findFiftyDayAverage(Deque<Double> movingAverage) {
		double average = 0;
		for (Double value : movingAverage) {
            average += value;
        }
		return average / 50;
	}

	public static void logMessage(String message) {
		try {
			writer.write(message);
			writer.flush();
		} catch (IOException e) {
			System.err.println("Logging failed: " + e.getMessage());
		}
	}
}
