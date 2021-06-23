import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class CsvParser {

	static String jdbcURL = "jdbc:mysql://localhost:3306/sales";
	static String username = "root";
	static String password = "";

	public CsvParser() {
		ArrayList<String> paths = new ArrayList<String>();
		paths.add("D:\\workspace\\CvsParser\\csv\\simple1.csv");
		paths.add("D:\\workspace\\CvsParser\\csv\\simple2.csv");
		paths.add("D:\\workspace\\CvsParser\\csv\\simple3.csv");
		paths.add("D:\\workspace\\CvsParser\\csv\\simple4.csv");

		for (String filePath : paths) {
			this.parseFileAndSaveToDB(filePath);
		}
	}

	public void parseFileAndSaveToDB(String filePath) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				int batchSize = 20;
				Connection connection = null;
				try {

					connection = DriverManager.getConnection(jdbcURL, username, password);
					connection.setAutoCommit(false);

					String sql = "INSERT INTO review (course_name, student_name, timestamp, rating, comment) VALUES (?, ?, ?, ?, ?)";
					PreparedStatement statement = connection.prepareStatement(sql);

					BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
					String lineText = null;

					int count = 0;

					lineReader.readLine(); // skip header line

					while ((lineText = lineReader.readLine()) != null) {
						String[] data = lineText.split(",");
						String courseName = data[0];
						String studentName = data[1];
						String timestamp = data[2];
						String rating = data[3];
						String comment = data.length == 5 ? data[4] : "";

						statement.setString(1, courseName);
						statement.setString(2, studentName);

						// Timestamp sqlTimestamp = Timestamp.valueOf(timestamp);
						statement.setString(3, timestamp);

						// Float fRating = Float.parseFloat(rating);
						statement.setString(4, rating);

						statement.setString(5, comment);

						statement.addBatch();

						if (count % batchSize == 0) {
							statement.executeBatch();
						}
					}

					lineReader.close();

					// execute the remaining queries
					statement.executeBatch();

					connection.commit();
					connection.close();

				} catch (IOException ex) {
					System.err.println(ex);
				} catch (SQLException ex) {
					ex.printStackTrace();

					try {
						connection.rollback();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

			}
		}).start();
	}

	public static void main(String[] args) {
		new CsvParser();
	}
}