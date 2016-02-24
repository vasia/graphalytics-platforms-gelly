package nl.tudelft.graphalytics.flink.algorithms.bfs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;

public class BreadthFirstSearchAlgorithm<K, VV, EV> implements
	GraphAlgorithm<K, VV, EV, DataSet<Tuple2<K, VV>>> {

	@Override
	public DataSet<Tuple2<K, VV>> run(Graph<K, VV, EV> arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
