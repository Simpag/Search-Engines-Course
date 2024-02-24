import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.io.*;

public class PageRank {

    /**  
     *   Maximal number of documents. We're assuming here that we
     *   don't have more docs than we can keep in main memory.
     */
    final static int MAX_NUMBER_OF_DOCS = 2000000;

    /**
     *   Mapping from document names to document numbers.
     */
    HashMap<String,Integer> docNumber = new HashMap<String,Integer>();

    /**
     *   Mapping from document numbers to document names
     */
    String[] docName = new String[MAX_NUMBER_OF_DOCS];

    /**  
     *   A memory-efficient representation of the transition matrix.
     *   The outlinks are represented as a HashMap, whose keys are 
     *   the numbers of the documents linked from.<p>
     *
     *   The value corresponding to key i is a HashMap whose keys are 
     *   all the numbers of documents j that i links to.<p>
     *
     *   If there are no outlinks from i, then the value corresponding 
     *   key i is null.
     */
    HashMap<Integer,HashMap<Integer,Boolean>> link = new HashMap<Integer,HashMap<Integer,Boolean>>();

    /**
     *   The number of outlinks from each node.
     */
    int[] out = new int[MAX_NUMBER_OF_DOCS];

    /**
     *   The probability that the surfer will be bored, stop
     *   following links, and take a random jump somewhere.
     */
    final static double BORED = 0.15;

    /**
     *   Convergence criterion: Transition probabilities do not 
     *   change more that EPSILON from one iteration to another.
     */
    final static double EPSILON = 0.0001;

       
    /* --------------------------------------------- */


    public PageRank( String filename ) {
		int noOfDocs = readDocs( filename );
		iterate( noOfDocs, 1000 );
    }


    /* --------------------------------------------- */


    /**
     *   Reads the documents and fills the data structures. 
     *
     *   @return the number of documents read.
     */
    int readDocs( String filename ) {
		int fileIndex = 0;
		try {
			System.err.print( "Reading file... " );
			BufferedReader in = new BufferedReader( new FileReader( filename ));
			String line;
			while ((line = in.readLine()) != null && fileIndex<MAX_NUMBER_OF_DOCS ) {
				int index = line.indexOf( ";" );
				String title = line.substring( 0, index );
				Integer fromdoc = docNumber.get( title );
				//  Have we seen this document before?
				if ( fromdoc == null ) {	
					// This is a previously unseen doc, so add it to the table.
					fromdoc = fileIndex++;
					docNumber.put( title, fromdoc );
					docName[fromdoc] = title;
				}
				// Check all outlinks.
				StringTokenizer tok = new StringTokenizer( line.substring(index+1), "," );
				while ( tok.hasMoreTokens() && fileIndex<MAX_NUMBER_OF_DOCS ) {
					String otherTitle = tok.nextToken();
					Integer otherDoc = docNumber.get( otherTitle );
					if ( otherDoc == null ) {
						// This is a previousy unseen doc, so add it to the table.
						otherDoc = fileIndex++;
						docNumber.put( otherTitle, otherDoc );
						docName[otherDoc] = otherTitle;
					}
					// Set the probability to 0 for now, to indicate that there is
					// a link from fromdoc to otherDoc.
					if ( link.get(fromdoc) == null ) {
						link.put(fromdoc, new HashMap<Integer,Boolean>());
					}
					if ( link.get(fromdoc).get(otherDoc) == null ) {
						link.get(fromdoc).put( otherDoc, true );
						out[fromdoc]++;
					}
				}
			}
			in.close();
			if ( fileIndex >= MAX_NUMBER_OF_DOCS ) {
				System.err.print( "stopped reading since documents table is full. " );
			}
			else {
				System.err.print( "done. " );
			}
		}
		catch ( FileNotFoundException e ) {
			System.err.println( "File " + filename + " not found!" );
		}
		catch ( IOException e ) {
			System.err.println( "Error reading file " + filename );
		}
		System.err.println( "Read " + fileIndex + " number of documents" );
		return fileIndex;
    }


    /* --------------------------------------------- */


	private void printTop30(double[] ranks) {
		int[] sortedIndices = IntStream.range(0, ranks.length)
                .boxed().sorted((i, j) -> Double.compare(ranks[j], ranks[i]) )
                .mapToInt(ele -> ele).toArray();

		for (int i = 0; i < 30; i++) {
			System.err.println(docName[sortedIndices[i]] + " : " + ranks[sortedIndices[i]]);
		}
	}

    /*
     *   Chooses a probability vector a, and repeatedly computes
     *   aP, aP^2, aP^3... until aP^i = aP^(i+1).
     */
    void iterate( int numberOfDocs, int maxIterations ) {
		// Init G Matrix
		double J = BORED * 1/numberOfDocs; // c = 1 - BORED
		HashMap<Integer,HashMap<Integer,Double>> G = createProbMatrix();
		matrixElementWiseMulti(G, 1-BORED);
		matrixElementWiseAdd(G, J);

		// Init state
		double[] x = new double[numberOfDocs];
		int randomState = -1;
		while (true) {
			randomState = ThreadLocalRandom.current().nextInt(0, numberOfDocs+1);
			if (link.containsKey(randomState))
				break;
		}

		System.err.println("Random state: " + randomState);

		x[randomState] = 1;
		double[] x_prev = new double[numberOfDocs];
		int iteration = 0;


		while (vectorDiffNorm1(x, x_prev) > EPSILON && iteration < maxIterations) {
			x_prev = x;
			x = matrixMultiply(G,x_prev);

			iteration++;
		}

		printTop30(x);
    }

	private HashMap<Integer,HashMap<Integer,Double>> createProbMatrix() {
		HashMap<Integer,HashMap<Integer,Double>> prob = new HashMap<Integer,HashMap<Integer,Double>>();
		
		for (int row : link.keySet()) {
			prob.put(row, new HashMap<Integer,Double>());

			double rowSum = 0;
			for (int col : link.get(row).keySet()) {
				if (prob.get(row).get(col) == null) {
					prob.get(row).put(col, (double)1/out[row]);
					rowSum += (double)1/out[row];
				}
			}

			if (!(Math.abs(1-rowSum) < 0.000001)) {
				System.err.println("Row did not sum to 1: " + rowSum);
			}
		}

		return prob;
	}

	private double vectorNorm1(double[] x) {
		double ret = 0;

		for (int i = 0; i < x.length; i++) {
			ret += x[i];
		}

		return ret;
	}

	private double vectorDiffNorm1(double[] x, double[] y) {
		double ret = 0;

		for (int i = 0; i < x.length; i++) {
			ret += Math.abs(x[i] - y[i]);
		}

		return ret;
	}

	private void matrixElementWiseAdd(HashMap<Integer,HashMap<Integer,Double>> prob, double adder) {
		for (int row : prob.keySet()) {
			for (int col : prob.get(row).keySet()) {
				prob.get(row).put(col, prob.get(row).get(col) + adder);
			}
		}
	}

	private void matrixElementWiseMulti(HashMap<Integer,HashMap<Integer,Double>> prob, double multi) {
		for (int row : prob.keySet()) {
			for (int col : prob.get(row).keySet()) {
				prob.get(row).put(col, prob.get(row).get(col) * multi);
			}
		}
	}

	private double[] matrixMultiply(HashMap<Integer,HashMap<Integer,Double>> prob, double[] x) {
		/// Takes a row vector and multiplies it with a matrix (x*A=x')
		double[] x_prime = new double[x.length];

		for (int row : prob.keySet()) {
			for (int col : prob.get(row).keySet()) { 
				if (prob.get(row).get(col) != null) {
					x_prime[col] += prob.get(row).get(col) * x[row];
				}
			}
		}

		return x_prime;
	}


    /* --------------------------------------------- */


    public static void main( String[] args ) {
		if ( args.length != 1 ) {
			System.err.println( "Please give the name of the link file" );
		} else {
			new PageRank( args[0] );
		}
    }
}