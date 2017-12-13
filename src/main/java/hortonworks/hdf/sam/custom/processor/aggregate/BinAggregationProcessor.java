package hortonworks.hdf.sam.custom.processor.aggregate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

import java.util.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class BinAggregationProcessor implements CustomProcessorRuntime {

    protected static final Logger LOG = LoggerFactory.getLogger(BinAggregationProcessor.class);

    static final String CONFIG_GROUP_BY_KEYS = "groupByKeys";
    static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
    static final String CONFIG_AGGREGATION_FIELDS = "aggregationFields";
    static final String CONFIG_AGGREGATION_FUNCTIONS = "aggregationFunctions";
    static final String CONFIG_BIN_SIZE = "binSize";
    static final String CONFIG_UPPER_LIMIT_FIELD = "upperLimitField";
    static final String CONFIG_LOWER_LIMIT_FIELD = "lowerLimitField";
    static final String CONFIG_SMOOTHING_FACTOR_FIELD = "smoothingFactorField";
    static final String CONFIG_HIVE_CONNECT_STRING = "hiveConnectString";
    static final String CONFIG_HIVE_TABLE = "hiveTable";
    static final String CONFIG_HIVE_SORT_BY_FIELD = "hiveSortByField";

    private String[] groupByKeys = null;
    private String[] enrichedOutputFields = null;
    private String[] aggregationFields = null;
    private String[] aggregationFunctions = null;
    private int binSize;
    private String upperLimitField = null;
    private String lowerLimitField = null;
    private String smoothingFactorField = null;
    private String hiveConnectString = null;
    private String hiveTable = null;
    private String hiveSortByField = null;
    private Map<String, List<StreamEvent>> aggregationMap = null;

    @Override
    public void validateConfig(Map<String, Object> config) throws ConfigException { }

    @Override
    public List<StreamlineEvent> process(StreamlineEvent streamlineEvent) throws ProcessingException {
        LOG.info("Event[" + streamlineEvent + "] about to be enriched");

        String id = "";
        for (int i=0; i < this.groupByKeys.length; i++) {
            Object o = streamlineEvent.get(this.groupByKeys[i]);
            id+=convertObjectToString(o);
        }

        LinkedList<StreamEvent> eventList = (LinkedList<StreamEvent>) this.aggregationMap.get(id);
        if (eventList == null) {
            eventList = new LinkedList<StreamEvent>();
            eventList.addLast(new StreamEvent(streamlineEvent,this.aggregationFields));
            this.aggregationMap.put(id,eventList);
        } else {
            if (eventList.size() < (binSize-1)) {
                eventList.addLast(new StreamEvent(streamlineEvent,this.aggregationFields));
            } else {
                eventList.addLast(new StreamEvent(streamlineEvent,this.aggregationFields));

                StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
                for (int i = 0; i < this.enrichedOutputFields.length; i++) {
                    builder.put(this.enrichedOutputFields[i], streamlineEvent.get(this.enrichedOutputFields[i]));
                }

                for (int i=0; i < this.aggregationFields.length; i++) {
                    double[] aggregationValues = new double[eventList.size()];

                    String aggregationField = this.aggregationFields[i];

                    for (int j = 0; j < eventList.size(); j++) {
                        aggregationValues[j] = ((StreamEvent) eventList.get(j)).getValue(aggregationField);
                    }

                    Statistics stats = new Statistics(aggregationValues);

                    for (int j = 0; j < this.aggregationFunctions.length; j++) {
                        if (this.aggregationFunctions[j].toLowerCase().equals("min")) {
                            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                                builder.put("min_" + aggregationField, (int)stats.getMin());
                            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                                builder.put("min_" + aggregationField, (float)stats.getMin());
                            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                                builder.put("min_" + aggregationField, stats.getMin());
                            }
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("max")) {
                            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                                builder.put("max_" + aggregationField, (int)stats.getMax());
                            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                                builder.put("max_" + aggregationField, (float)stats.getMax());
                            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                                builder.put("max_" + aggregationField, stats.getMax());
                            }
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("mean")) {
                            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                                builder.put("mean_" + aggregationField, (int)stats.getMean());
                            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                                builder.put("mean_" + aggregationField, (float)stats.getMean());
                            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                                builder.put("mean_" + aggregationField, stats.getMean());
                            }
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("stddev")) {
                            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                                builder.put("stddev_" + aggregationField, (int)stats.getStandardDeviation());
                            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                                builder.put("stddev_" + aggregationField, (float)stats.getStandardDeviation());
                            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                                builder.put("stddev_" + aggregationField, stats.getStandardDeviation());
                            }
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("zscore")) {
                            double zscore = (eventList.getLast().getValue(aggregationField)-stats.getMean())/stats.getStandardDeviation();
                            if (streamlineEvent.get(aggregationField) instanceof Integer) {
                                builder.put("zscore_" + aggregationField, (int)zscore);
                            } else if (streamlineEvent.get(aggregationField) instanceof Float) {
                                builder.put("zscore_" + aggregationField, (float)zscore);
                            } else if (streamlineEvent.get(aggregationField) instanceof Double) {
                                builder.put("zscore_" + aggregationField, zscore);
                            }
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("cpk")) {
                            double lowerLimit = 0.d;
                            if (streamlineEvent.get(this.lowerLimitField) instanceof Integer) {
                                lowerLimit = ((Integer)streamlineEvent.get(this.lowerLimitField)).doubleValue();
                            } else if (streamlineEvent.get(this.lowerLimitField) instanceof Float) {
                                lowerLimit = ((Float)streamlineEvent.get(this.lowerLimitField)).doubleValue();
                            } else if (streamlineEvent.get(this.lowerLimitField) instanceof Double) {
                                lowerLimit = ((Double)streamlineEvent.get(this.lowerLimitField)).doubleValue();
                            }

                            double upperLimit = 0.d;
                            if (streamlineEvent.get(this.upperLimitField) instanceof Integer) {
                                upperLimit = ((Integer)streamlineEvent.get(this.upperLimitField)).doubleValue();
                            } else if (streamlineEvent.get(this.upperLimitField) instanceof Float) {
                                upperLimit = ((Float)streamlineEvent.get(this.upperLimitField)).doubleValue();
                            } else if (streamlineEvent.get(this.upperLimitField) instanceof Double) {
                                upperLimit = ((Double)streamlineEvent.get(this.upperLimitField)).doubleValue();
                            }

                            double cpk = java.lang.Math.min(stats.getMean()-lowerLimit, upperLimit-stats.getMean())/(3.d*stats.getStandardDeviation());
                            builder.put("cpk_" + aggregationField, cpk);
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("count")) {
                            int count = 0;
                            for (StreamEvent e : eventList) {
                                int value = e.getValue(aggregationField).intValue();
                                if (value != 0) {
                                    count++;
                                }
                            }
                            builder.put("count_" + aggregationField, ((double)count)/eventList.size()*100);
                        } else if (this.aggregationFunctions[j].toLowerCase().equals("exps")) {
                            double smoothingFactor = 0.d;
                            if (streamlineEvent.get(this.smoothingFactorField) instanceof Integer) {
                                smoothingFactor = ((Integer) streamlineEvent.get(this.smoothingFactorField)).doubleValue();
                            } else if (streamlineEvent.get(this.smoothingFactorField) instanceof Float) {
                                smoothingFactor = ((Float) streamlineEvent.get(this.smoothingFactorField)).doubleValue();
                            } else if (streamlineEvent.get(this.smoothingFactorField) instanceof Double) {
                                smoothingFactor = ((Double) streamlineEvent.get(this.smoothingFactorField)).doubleValue();
                            }

                            double y = aggregationValues[0];
                            for (int k=1; k < aggregationValues.length; k++) {
                               y = smoothingFactor * aggregationValues[k] + (1.d - smoothingFactor) * y;
                            }
                            builder.put("exps_" + aggregationField, y);
                        }
                    }
                }

                eventList.removeFirst();

                StreamlineEvent enrichedEvent = builder.dataSourceId(streamlineEvent.getDataSourceId()).build();
                LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

                List<StreamlineEvent> newEvents = Collections.<StreamlineEvent>singletonList(enrichedEvent);
                return newEvents;
            }
        }

        return null;
    }

    private String convertObjectToString(Object o) {
        if (o instanceof String) {
            return (String)o;
        } else if (o instanceof Integer) {
            return (String) ((Integer)o).toString();
        } else if (o instanceof Float) {
            return (String) ((Float)o).toString();
        } else if (o instanceof Double) {
            return (String) ((Double)o).toString();
        } else if (o instanceof Boolean) {
            return (String) ((Boolean)o).toString();
        }
        return "";
    }

    @Override
    public void initialize(Map<String, Object> config) {
        LOG.info("Initializing + " + BinAggregationProcessor.class.getName());

        this.aggregationMap = new HashMap<String, List<StreamEvent>>();

        String keys = ((String) config.get(CONFIG_GROUP_BY_KEYS)).trim();
        this.groupByKeys = keys.split(",");

        String enrichedOutputFields = ((String) config.get(CONFIG_ENRICHED_OUTPUT_FIELDS)).trim();
        this.enrichedOutputFields = enrichedOutputFields.split(",");

        String aggregationFields = ((String) config.get(CONFIG_AGGREGATION_FIELDS)).trim();
        this.aggregationFields = aggregationFields.split(",");

        String functions = ((String) config.get(CONFIG_AGGREGATION_FUNCTIONS)).trim();
        this.aggregationFunctions = functions.split(",");

        this.binSize = ((Integer) config.get(CONFIG_BIN_SIZE)).intValue();

        this.upperLimitField = ((String) config.get(CONFIG_UPPER_LIMIT_FIELD)).trim();

        this.lowerLimitField = ((String) config.get(CONFIG_LOWER_LIMIT_FIELD)).trim();

        this.smoothingFactorField = ((String) config.get(CONFIG_SMOOTHING_FACTOR_FIELD)).trim();

        this.hiveConnectString = ((String) config.get(CONFIG_HIVE_CONNECT_STRING)).trim();

        this.hiveTable = ((String) config.get(CONFIG_HIVE_TABLE)).trim();

        this.hiveSortByField = ((String) config.get(CONFIG_HIVE_SORT_BY_FIELD)).trim();

        try {
            initializeBinsFromHive();
        } catch (SQLException e) {
            LOG.info(e.getMessage(),e);
        }

        LOG.info("Initialized + " + BinAggregationProcessor.class.getName());
    }

    private void initializeBinsFromHive() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            LOG.info(e.getMessage(),e);
            System.exit(1);
        }

        Connection con = null;
        Statement stmt=null;
        ResultSet res=null;
        try {
            con = DriverManager.getConnection("jdbc:" + this.hiveConnectString, "hive", "");
            stmt = con.createStatement();
            res = stmt.executeQuery("select * from " + this.hiveTable + " sort by " + this.hiveSortByField + " desc");
            while (res.next()) {
                StreamEvent e = new StreamEvent();
                for (String field : this.aggregationFields) {
                    Object o = res.getObject(field);
                    Double value = null;
                    if (o instanceof Integer) {
                        value = new Double(((Integer) o).doubleValue());
                    } else if (o instanceof Float) {
                        value = new Double(((Float) o).doubleValue());
                    } else if (o instanceof Double) {
                        value = (Double) o;
                    }
                    e.putValue(field, value);
                }

                String id = "";
                for (String key : this.groupByKeys) {
                    Object o = res.getObject(key);
                    id += convertObjectToString(o);
                }

                LinkedList<StreamEvent> eventList = (LinkedList<StreamEvent>) this.aggregationMap.get(id);
                if (eventList == null) {
                    eventList = new LinkedList<StreamEvent>();
                    eventList.addFirst(e);
                    this.aggregationMap.put(id, eventList);
                } else {
                    if (eventList.size() < (this.binSize-1)) {
                        eventList.addFirst(e);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.info(e.getMessage(),e);
        } finally{
            res.close();
            stmt.close();
            con.close();
        }
    }

    @Override
    public void cleanup() {
        this.aggregationMap.clear();
    }
}
