package hortonworks.hdf.sam.custom.processor.aggregate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Statistics {
    protected static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
    private double[] values;

    public Statistics(double[] values) {
        this.values=values;
    }

    public double getMax() {
        double max = Double.MIN_VALUE;
        for (double v : this.values) {
            if (v > max) {
                max = v;
            }
        }
        return max;
    }

    public double getMin() {
        double min = Double.MAX_VALUE;
        for (double v : this.values) {
            if (v < min) {
                min = v;
            }
        }
        return min;
    }

    public double getMean() {
        double sum = 0.d;
        for (double v : this.values) {
            sum+=v;
        }
        return sum/(double)this.values.length;
    }

    public double getVariance() {
        double mean = getMean();
        double sum = 0.d;
        for (double v : this.values) {
            sum+=((v-mean)*(v-mean));
        }
        return sum/(double)(this.values.length-1);
    }

    public double getStandardDeviation() {
        double var = getVariance();
        return java.lang.Math.sqrt(var);
    }
}
