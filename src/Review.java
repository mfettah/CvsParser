import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Review {

	private String course_name;
	private String student_name;
	private String rating;
	private String comment;
	private Timestamp timestamp;

	public Review() {
	}

	public String getCourse_name() {
		return course_name;
	}

	public void setCourse_name(String course_name) {
		this.course_name = course_name;
	}

	public String getStudent_name() {
		return student_name;
	}

	public void setStudent_name(String student_name) {
		this.student_name = student_name;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			Date date = dateFormat.parse(timestamp);
			this.timestamp = new Timestamp(date.getTime());
		} catch (ParseException e) {
			System.out.println(timestamp);
			e.printStackTrace();
		}
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

}
