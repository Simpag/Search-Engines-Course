package com.motecarlo;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import java.io.*;
import java.text.DecimalFormat;  

public class MonteCarloSV {

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


    public MonteCarloSV() {
		String filename = "C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/linksSvwiki.txt";
		int noOfDocs = readDocs( filename );
		iterate( noOfDocs );
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
		int[] sortedIndices = getTop30Indicies(ranks);

		DecimalFormat df = new DecimalFormat("#.#####");
		for (int i = 0; i < 30; i++) {
			System.err.println(docName[sortedIndices[i]] + ": " + df.format(ranks[sortedIndices[i]]));
		}
	}

    private int[] getTop30Indicies(double[] ranks) {
		int[] sortedIndices = IntStream.range(0, ranks.length)
                .boxed().sorted((i, j) -> Double.compare(ranks[j], ranks[i]) )
                .mapToInt(ele -> ele).toArray();

        int[] ret = new int[30];

        for (int i = 0; i < 30; i++)
            ret[i] = sortedIndices[i];

		return ret;
	}

    private void writeRankings(double[] ranks, String filename) {
		HashMap<String,String> nameToFile = new HashMap<String,String>();
		try {
			BufferedReader in = new BufferedReader( new FileReader("C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/svwikiTitles.txt"));
			String line;
			while ((line = in.readLine()) != null) {
				int index = line.indexOf( ";" );
				String name = line.substring(0, index );
				String file = line.substring(index +1, line.length());
				nameToFile.put(name,file);
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Write rankings
		int[] sortedIndices = IntStream.range(0, ranks.length)
                .boxed().sorted((i, j) -> Double.compare(ranks[j], ranks[i]) )
                .mapToInt(ele -> ele).toArray();
				
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			// Maybe read in the titles?
			for (int i = 0; i < ranks.length; i++) {
				writer.write(nameToFile.get(docName[sortedIndices[i]]) + ";" + ranks[sortedIndices[i]] + "\n");
			}

			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

    /*
     *   Chooses a probability vector a, and repeatedly computes
     *   aP, aP^2, aP^3... until aP^i = aP^(i+1).
     */
    void iterate( int numberOfDocs ) {
		int averages = 10;
		int time = 1000;
		int m = 5;
        double[] pi = new double[numberOfDocs];
        double[] top30 = new double[30];

        int maxIterations = 10;
        int iterations = 0;
        boolean firstAdd = true;
        while (iterations < maxIterations) {
            ArrayList<double[]> avgs = new ArrayList<double[]>();
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int t = 0; t < averages; t++) {
                final double[] avg = new double[numberOfDocs];
                avgs.add(avg);
                Thread thread = new Thread(() -> {
                    double[] _pi = Alg4(numberOfDocs*m, time, numberOfDocs);
                    addTwoVectors(avg, _pi);
                });
                thread.start();
                threads.add(thread);
            }

            for (Thread t : threads) {
                try {
                    t.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            for (int i = 1; i < avgs.size(); i++) {
                addTwoVectors(avgs.get(0), avgs.get(i));
            }
            vectorMulti(avgs.get(0), 1.0/averages);
            addTwoVectors(pi, avgs.get(0));

            if (!firstAdd)
                vectorMulti(pi, 1.0/2.0);

            double[] pi30 = new double[30];
            int[] pi30i = getTop30Indicies(pi);
            for (int i = 0; i < 30; i++) {
                pi30[i] = pi30i[i];
            }
            double diff = vectorDiffNorm1(pi30, top30);
            top30 = pi30;
            firstAdd = false;
            iterations++;
            System.err.println("Iteration: " + iterations + "; Norm: " + vectorNorm1(pi) + "; Diff: " + diff);
            if (diff < EPSILON)
                break;
        }

        System.err.println("Finalnorm: " + vectorNorm1(pi));
        printTop30(pi);
        writeRankings(pi, "my_sv_rankings.txt");
    }

	private double[] Alg4(int N, int time, int numberOfDocs) {
		if (N%numberOfDocs != 0)
			throw new IllegalArgumentException("numberOfDocs must be a divisor of N!");

		double[] pi = new double[numberOfDocs];
		int numberOfWalks = 0;
		
		int n = 0;
		while (n < N) {
			int state = n%numberOfDocs;
			pi[state] += 1.0;
			numberOfWalks++;
			int t = 0;
			while (t++ < time) {
				double r1 = ThreadLocalRandom.current().nextDouble();
				
				if (r1 < BORED)
					break;

				if (out[state] < 1) // At a sink
					break;

				int nextState = ThreadLocalRandom.current().nextInt(0, out[state]);
				state = (int)link.get(state).keySet().toArray()[nextState];
				numberOfWalks++;
				pi[state] += 1.0;
			}
			n++;
		}

		vectorMulti(pi, 1.0/numberOfWalks);

		return pi;
	}

    private double vectorDiffNorm1(double[] x, double[] y) {
		double ret = 0;

		for (int i = 0; i < x.length; i++) {
			ret += Math.abs(x[i] - y[i]);
		}

		return ret;
	}

	private void addTwoVectors(double[] x, double[] y) {
		/* Adds y to x */
		if (x.length != y.length)
			throw new IllegalArgumentException("x.length and y.length must be the same!");

		//double[] res = new double[x.length];

		for (int i = 0; i < x.length; i++) {
			//res[i] = x[i] + y[i];
			x[i] += y[i];
		}

		//return res;
	}

	private void vectorMulti(double[] x, double y) {
		//double[] res = new double[x.length];

		for (int i = 0; i < x.length; i++) {
			//res[i] = x[i] * y;
			x[i] *= y;
		}
	}

	private double vectorNorm1(double[] x) {
		double ret = 0;

		for (int i = 0; i < x.length; i++) {
			ret += Math.abs(x[i]);
		}

		return ret;
	}

    /* --------------------------------------------- */


    public static void main( String[] args ) {
		new MonteCarloSV();
    }
}


