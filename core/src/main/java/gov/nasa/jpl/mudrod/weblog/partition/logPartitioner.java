package gov.nasa.jpl.mudrod.weblog.partition;

import org.apache.spark.Partitioner;

import java.util.Map;

public class logPartitioner extends Partitioner {

  int num;
  Map<String, Integer> UserGroups;

  public logPartitioner(int num) {
    this.num = num;
  }

  public logPartitioner(Map<String, Integer> UserGroups, int num) {
    this.UserGroups = UserGroups;
    this.num = num;
  }

  @Override
  public int getPartition(Object arg0) {
    // TODO Auto-generated method stub
    String user = (String) arg0;
    return UserGroups.get(user);
  }

  @Override
  public int numPartitions() {
    // TODO Auto-generated method stub
    return num;
  }
}
