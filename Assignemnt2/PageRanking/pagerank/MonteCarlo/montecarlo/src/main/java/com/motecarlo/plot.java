package com.motecarlo;
import java.util.*;

import javax.swing.JFrame;  
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.jfree.chart.ChartFactory;  
import org.jfree.chart.ChartPanel;  
import org.jfree.chart.JFreeChart;  
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class plot extends JFrame {
    public plot(String title, ArrayList<Double> x, HashMap<String, ArrayList<Double>> y, boolean loglogscale) {  
        super(title);  
        // Create dataset  
        XYSeriesCollection dataset = createDataset(x, y, loglogscale);  
        // Create chart  
        String xlabel = loglogscale ? "log_10(m) (N=m*numberOfDocs)" : "m (N=m*numberOfDocs)";
        String ylabel = loglogscale ? "log_10(Goodness)" : "Goodness";
        JFreeChart chart = ChartFactory.createXYLineChart(  
            title, // Chart title  
            xlabel, // X-Axis Label  
            ylabel, // Y-Axis Label  
            dataset  
            );  
      
        ChartPanel panel = new ChartPanel(chart);  
        setContentPane(panel);  
      }  
      
      private XYSeriesCollection createDataset(ArrayList<Double> x, HashMap<String, ArrayList<Double>> y, boolean loglogscale) {  
        XYSeriesCollection dataset = new XYSeriesCollection();  
      
        for (String alg : y.keySet()) {
            XYSeries series = new XYSeries(alg);
            for (int i = 0; i < x.size(); i++) {
                double xx = x.get(i);
                double yy = y.get(alg).get(i);

                if (loglogscale) {
                    xx = Math.log10(xx);
                    yy = Math.log10(yy);
                }

                series.add(xx,yy);
            }
            dataset.addSeries(series);
        }
      
        return dataset;  
      }  
}
