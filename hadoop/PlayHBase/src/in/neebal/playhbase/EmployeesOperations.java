package in.neebal.playhbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class EmployeesOperations {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Connection connection = ConnectionFactory.createConnection();
		 Table table = connection.getTable(TableName.valueOf("employees"));
		 try {
			 Put put = new Put(Bytes.toBytes("rish-9243"));
			 put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("rishabh"));
			 put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("gender"), Bytes.toBytes("M"));
			 table.put(put);
			 
			 Get get = new Get(Bytes.toBytes("rish-9243"));
			 get.addFamily(Bytes.toBytes("personal"));
			 Result result = table.get(get);
			 System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"))));
		 } finally {
		   table.close();
		   connection.close();
		 }
	}

}
