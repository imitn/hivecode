import java.util.List;
 
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
 
public class DescribeHiveTableHandler {
 
    public static void main(String[] args) {
      
       String thriftHiveMetastoreHost = "localhost"; // Your Hive Metastore Server Host Name
       int thriftHiveMetastorePort = 9083; // Your Hive Metastore Server Port
 
       TSocket transport = new TSocket(thriftHiveMetastoreHost, thriftHiveMetastorePort);
       Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
 
       try {
           transport.open();
          
           Table t = client.get_table("schame", "table");
           StorageDescriptor storageDescriptor = t.getSd();
 
           String location = storageDescriptor.getLocation();
           System.out.println("the location of the table is "+location);
          
           String inputFormat=storageDescriptor.getInputFormat();
           System.out.println("the inputFormat of the table is "+inputFormat);
          
           List<FieldSchema> cols = storageDescriptor.getCols();
           for (FieldSchema fs : cols) {
              System.out.print("column name:"+fs.getName());
              System.out.println(",column type:"+fs.getType());
           }
 
       } catch (TException e) {
           System.out.println("TSocket open failed");
           e.printStackTrace();
       }finally {
           if (transport != null) {
              transport.close();
           }
       }
    }
 
}
 
