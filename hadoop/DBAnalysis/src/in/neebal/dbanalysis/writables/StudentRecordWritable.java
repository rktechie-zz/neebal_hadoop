package in.neebal.dbanalysis.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class StudentRecordWritable implements DBWritable, Writable {

	private LongWritable id;
	private Text studentName;
	private Text collegeAddress;
	
	public StudentRecordWritable(){
		id = new LongWritable();
		studentName = new Text();
		collegeAddress = new Text();
	}
	
	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public Text getStudentName() {
		return studentName;
	}

	public void setStudentName(Text studentName) {
		this.studentName = studentName;
	}

	public Text getCollegeAddress() {
		return collegeAddress;
	}

	public void setCollegeAddress(Text collegeAddress) {
		this.collegeAddress = collegeAddress;
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		id.set(rs.getLong("id"));
		studentName.set(rs.getString("student_name"));
		collegeAddress.set(rs.getString("college_address"));
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, studentName.toString());
		ps.setString(2, collegeAddress.toString());
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		id.readFields(input);
		studentName.readFields(input);
		collegeAddress.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		id.write(output);
		studentName.write(output);
		collegeAddress.write(output);
	}

}
