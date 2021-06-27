import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DurationFormatUtils;

public class CsvParser {

	static String jdbcURL = "jdbc:mysql://localhost:3306/sales";
	static String username = "root";
	static String password = "";
	int count = 0;

	public CsvParser() throws IOException {
		ArrayList<String> paths = new ArrayList<String>();
		paths.add("D:\\workspace\\CvsParser\\csv\\simple1.csv");
		/*
		 * paths.add("D:\\workspace\\CvsParser\\csv\\simple2.csv");
		 * paths.add("D:\\workspace\\CvsParser\\csv\\simple3.csv");
		 * paths.add("D:\\workspace\\CvsParser\\csv\\simple4.csv");
		 */
		for (String filePath : paths) {
			this.parseFileAndSaveToDB(filePath);
		}
	}

	public void parseFileAndSaveToDB(String filePath) throws IOException {
		/*
		 * new Thread(new Runnable() {
		 *
		 * @Override public void run() {
		 */

		long lineCount;
		int count = 0;
		Path path = Paths.get(filePath);

		try (Stream<String> stream = Files.lines(path)) {
			lineCount = stream.count() - 1;
		}

		int batchSize = 100;
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(jdbcURL, username, password);
			connection.setAutoCommit(false);
			String sql = "INSERT INTO review (course, name, date, rating, comment) VALUES (?, ?, ?, ?, ?)";
			PreparedStatement pstm = connection.prepareStatement(sql);

			long start = System.currentTimeMillis();

			Set<String> hset = Files.lines(path).collect(Collectors.toSet());
			hset.remove("Course,Student,Date,Rating,Comment");
			Iterator<String> it = hset.iterator();
			it.next();
			while (it.hasNext()) {
				String[] data = it.next().split(",");
				this.init(pstm, data);
				if (count % batchSize == 0) {
					pstm.executeBatch();
					//pstm.clearParameters();//*********************
				}
			}

			pstm.executeBatch();
			pstm.close();

			connection.commit();
			long finish = System.currentTimeMillis();
			long duration = finish - start;
			System.out
					.println("durationInMillis : " + DurationFormatUtils.formatDuration(duration, "H:mm:ss:SSS", true));
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

		/*
		 * } }).start();
		 */
	}

	public void init(PreparedStatement pstm, String[] data) throws SQLException {
		Review rev = new Review();

		rev.setCourse_name(data[0]);
		rev.setStudent_name(data[1]);
		rev.setTimestamp(data[2]);
		rev.setRating(data[3]);
		rev.setComment(data.length == 5 ? data[4] : "");

		pstm.setString(1, rev.getCourse_name());
		pstm.setString(2, rev.getStudent_name());
		pstm.setTimestamp(3, rev.getTimestamp());
		pstm.setString(4, rev.getRating());
		pstm.setString(5, rev.getComment());
		pstm.addBatch();
	}

	public static void main(String[] args) {
		try {
			new CsvParser();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}