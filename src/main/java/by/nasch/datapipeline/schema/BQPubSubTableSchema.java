package by.nasch.datapipeline.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.Arrays;

public class BQPubSubTableSchema {
    public static TableSchema createSchema() {
        TableSchema schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema().setName("id").setType("INTEGER"),
                                        new TableFieldSchema().setName("date_time").setType("STRING"),
                                        new TableFieldSchema().setName("site_name").setType("INTEGER"),
                                        new TableFieldSchema().setName("posa_continent").setType("INTEGER"),
                                        new TableFieldSchema().setName("user_location_country").setType("INTEGER"),
                                        new TableFieldSchema().setName("user_location_region").setType("INTEGER"),
                                        new TableFieldSchema().setName("user_location_city").setType("INTEGER"),
                                        new TableFieldSchema().setName("orig_destination_distance").setType("FLOAT"),
                                        new TableFieldSchema().setName("user_id").setType("INTEGER"),
                                        new TableFieldSchema().setName("is_mobile").setType("INTEGER"),
                                        new TableFieldSchema().setName("is_package").setType("INTEGER"),
                                        new TableFieldSchema().setName("channel").setType("INTEGER"),
                                        new TableFieldSchema().setName("srch_ci").setType("STRING"),
                                        new TableFieldSchema().setName("srch_co").setType("STRING"),
                                        new TableFieldSchema().setName("srch_adults_cnt").setType("INTEGER"),
                                        new TableFieldSchema().setName("srch_children_cnt").setType("INTEGER"),
                                        new TableFieldSchema().setName("srch_rm_cnt").setType("INTEGER"),
                                        new TableFieldSchema().setName("srch_destination_id").setType("INTEGER"),
                                        new TableFieldSchema().setName("srch_destination_type_id").setType("INTEGER"),
                                        new TableFieldSchema().setName("hotel_id").setType("INTEGER")));
        return schema;
    }
}
