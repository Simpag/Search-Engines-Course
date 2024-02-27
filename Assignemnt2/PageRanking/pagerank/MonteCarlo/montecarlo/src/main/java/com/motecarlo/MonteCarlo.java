package com.motecarlo;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import java.io.*;
import java.text.DecimalFormat;  

public class MonteCarlo {

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

	HashMap<String, Double> topRankings = new HashMap<String, Double>();

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


    public MonteCarlo() {
		String filename = "C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/linksDavis.txt";
		int noOfDocs = readDocs( filename );
		readTopRankings();
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

	private void readTopRankings() {
		String filename = "C:/Users/Simon/Documents/Github/Search-Engines-Course/Assignemnt2/PageRanking/pagerank/top_30/davis_top_30.txt";
		try {
			BufferedReader in = new BufferedReader( new FileReader(filename));
			String line;
			while ((line = in.readLine()) != null) {
				int index = line.indexOf( ":" );
				String title = line.substring( 0, index );
				Double ranking = Double.parseDouble(line.substring(index+2));
				topRankings.put(title, ranking);
			}	
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


    /* --------------------------------------------- */

    /*
     *   Chooses a probability vector a, and repeatedly computes
     *   aP, aP^2, aP^3... until aP^i = aP^(i+1).
     */
    void iterate( int numberOfDocs ) {
		int averages = 10;
		int time = 1000;
		int maxM = 100;
		int numM = 30;

		Integer[] space = getLogSpace(1, maxM, numM);

		System.err.println("m space: ");
		for (int i : space){
			System.err.print(i + ", ");
		}
		System.err.println(" ");
		
		ArrayList<Double> Ns = new ArrayList<Double>();
		HashMap<String, ArrayList<Double>> data = new HashMap<String, ArrayList<Double>>();
		
		for (int m : space) {
			HashMap<String, Double> algorithms = runAlgs(numberOfDocs, averages, time, m);

			for (String alg : algorithms.keySet()) {
				if (!data.containsKey(alg))
					data.put(alg, new ArrayList<Double>());

				data.get(alg).add(algorithms.get(alg));
			}

			Ns.add((double)m);
		}

		showPlot("Monte-Carlo-loglog", Ns, data, true);
		showPlot("Monte-Carlo", Ns, data, false);
    }

	private Integer[] getLogSpace(int start, int stop, int num) {
        Set<Integer> result = new HashSet<Integer>();
        double base = Math.pow(stop / start, 1.0 / (num - 1));

        for (int i = 0; i < num; i++) {
            result.add((int)Math.round(start * Math.pow(base, i)));
        }

		Integer[] ret = result.toArray(new Integer[result.size()]);
		Arrays.sort(ret);
		return ret;
	}

	private void showPlot(String title, ArrayList<Double> x, HashMap<String, ArrayList<Double>> y, boolean loglogscale) {  
        SwingUtilities.invokeLater(() -> {  
        plot example = new plot(title, x, y, loglogscale);  
        example.setAlwaysOnTop(true);  
        example.pack();  
        example.setSize(1200, 800);  
        example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
        example.setVisible(true);  
        });  
    }  

	private double computeGoodness(double[] x) {
		double s = 0;
		for (String doc : topRankings.keySet()) {
			int docN = docNumber.get(doc);

			s += Math.pow(topRankings.get(doc) - x[docN], 2.0);
		}

		return s;
	}

	private HashMap<String, Double> runAlgs(int numberOfDocs, int averages, int time, int m) {
		HashMap<String, Double> algorithms = new HashMap<String, Double>();

		System.err.println("Starting m: " + m);

		//System.err.print("Starting Alg1.... ");
		final double[] avg1 = new double[numberOfDocs];
		Thread t1 = new Thread(() -> {
			for (int i = 0; i < averages; i++) {
				double[] pi = Alg1(numberOfDocs*m, time, numberOfDocs);
				addTwoVectors(avg1, pi);
			}
			vectorMulti(avg1, 1.0/averages);
		});
		t1.start();
		//System.err.println("Finished Alg1! m: " + m);

		//System.err.print("Starting Alg2.... ");
		final double[] avg2 = new double[numberOfDocs];
		Thread t2 = new Thread(() -> {
			for (int i = 0; i < averages; i++) {
				double[] pi = Alg2(numberOfDocs*m, time, numberOfDocs);
				addTwoVectors(avg2, pi);
			}
			vectorMulti(avg2, 1.0/averages);
		});
		t2.start();
		//System.err.println("Finished Alg2! m: " + m);

		//System.err.print("Starting Alg4.... ")
		final double[] avg4 = new double[numberOfDocs];
		Thread t4 = new Thread(() -> {
			for (int i = 0; i < averages; i++) {
				double[] pi = Alg4(numberOfDocs*m, time, numberOfDocs);
				addTwoVectors(avg4, pi);
			}
			vectorMulti(avg4, 1.0/averages);
		});
		t4.start();
		//System.err.println("Finished Alg4! m: " + m);

		//System.err.print("Starting Alg5.... ");
		final double[] avg5 = new double[numberOfDocs];
		Thread t5 = new Thread(() -> {
			for (int i = 0; i < averages; i++) {
				double[] pi = Alg5(numberOfDocs*m, time, numberOfDocs);
				addTwoVectors(avg5, pi);
			}
			vectorMulti(avg5, 1.0/averages);
		});
		t5.start();
		//System.err.println("Finished Alg5! m: " + m);

		try {
			t1.join();
			t2.join();
			t4.join();
			t5.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
		algorithms.put("Alg1", computeGoodness(avg1));
		algorithms.put("Alg2", computeGoodness(avg2));
		algorithms.put("Alg4", computeGoodness(avg4));
		algorithms.put("Alg5", computeGoodness(avg5));

		System.err.println("Finished m: " + m);

		return algorithms;
	}

	private double[] Alg1(int N, int time, int numberOfDocs) {
		double[] pi = new double[numberOfDocs];
		
		int n = 0;
		while (n++ < N) {
			int state = ThreadLocalRandom.current().nextInt(0, numberOfDocs);
			int t = 0;
			while (t++ < time) {
				double r1 = ThreadLocalRandom.current().nextDouble();
				
				if (r1 < BORED)
					break;

				if (out[state] < 1) { // At a sink
					state = ThreadLocalRandom.current().nextInt(0, numberOfDocs);
					continue;
				}

				int nextState = ThreadLocalRandom.current().nextInt(0, out[state]);
				state = (int)link.get(state).keySet().toArray()[nextState];
			}
			pi[state] += 1.0;
		}

		vectorMulti(pi, 1.0/N);

		return pi;
	}

	private double[] Alg2(int N, int time, int numberOfDocs) {
		if (N%numberOfDocs != 0)
			throw new IllegalArgumentException("numberOfDocs must be a divisor of N!");

		double[] pi = new double[numberOfDocs];
		
		int n = 0;
		while (n < N) {
			int state = n%numberOfDocs;
			int t = 0;
			while (t++ < time) {
				double r1 = ThreadLocalRandom.current().nextDouble();
				
				if (r1 < BORED)
					break;

				if (out[state] < 1) { // At a sink
					state = ThreadLocalRandom.current().nextInt(0, numberOfDocs);
					continue;
				}

				int nextState = ThreadLocalRandom.current().nextInt(0, out[state]);
				state = (int)link.get(state).keySet().toArray()[nextState];
			}
			pi[state] += 1.0;
			n++;
		}

		vectorMulti(pi, 1.0/N);

		return pi;
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

	private double[] Alg5(int N, int time, int numberOfDocs) {
		double[] pi = new double[numberOfDocs];
		int numberOfWalks = 0;
		
		int n = 0;
		while (n < N) {
			int state = ThreadLocalRandom.current().nextInt(0, numberOfDocs);
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
		new MonteCarlo();
    }
}

