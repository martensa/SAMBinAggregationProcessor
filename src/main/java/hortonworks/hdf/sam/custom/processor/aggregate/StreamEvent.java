package hortonworks.hdf.sam.custom.processor.aggregate;

import com.hortonworks.streamline.streams.StreamlineEvent;

import java.util.HashMap;
import java.util.Map;

public class StreamEvent {
    private Map<String, Double> aggregationValues;

    public StreamEvent() {
        this.aggregationValues = new HashMap<String, Double>();
    }

    public StreamEvent(StreamlineEvent streamlineEvent, String[] aggregationFields) {
        this.aggregationValues = new HashMap<String, Double>();
        for (String aggregationField : aggregationFields) {
            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                this.aggregationValues.put(aggregationField,new Double(((Integer) streamlineEvent.get(aggregationField)).doubleValue()));
            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                this.aggregationValues.put(aggregationField,new Double(((Float) streamlineEvent.get(aggregationField)).doubleValue()));
            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                this.aggregationValues.put(aggregationField,(Double)streamlineEvent.get(aggregationField));
            }
        }
    }

    public void putValue(String v, Double d) {
        this.aggregationValues.put(v,d);
    }

    public Double getValue(String v) {
        return this.aggregationValues.get(v);
    }
}
