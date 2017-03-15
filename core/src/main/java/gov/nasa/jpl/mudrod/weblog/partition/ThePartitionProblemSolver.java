package gov.nasa.jpl.mudrod.weblog.partition;

import java.util.Map;

public interface ThePartitionProblemSolver {

  public Map<String, Integer> solve(Map<String, Double> labelNums, int k);
}
