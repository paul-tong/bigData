object Demo {
	def main(args: Array[String]) {
		"""val K = 3
		var array = new Array[Int](K)
		for (i <- 0 to K - 1) {
			array(i) = i + 1
		}

		for (num <- array) {
			println(num)
		}

		// build a graph<>
		var graph: Map[Int, Array[Int]] = Map()
		for (i <- 1 to K) {
			var start: Int = (i - 1) * K + 1
			graph += (start -> new Array[Int](K - 1))

			for (j <- 1 to K - 1) {
				graph(start)(j - 1) = start + j
			}
		}

		graph.keys.foreach{ i =>
			print("key is: " + i)
			print(" values is: ")
			for (num <- graph(i)) {
				print(num + ", ")
			}

			println()
		}
		println(graph.values)"""

		// build the graph<start, end>
		val K = 3
		var graph = scala.collection.mutable.Map[Int, Int]()
		for (i <- 1 to K*K) {
			if (i % K == 0) { // dandling vertice, point to dummy node
				graph += (i -> 0)
			}
			else {
				graph += (i -> (i + 1))
			}
		}

		graph.keys.foreach{i =>
			println(i + " -> " + graph(i))
		}
	}
}