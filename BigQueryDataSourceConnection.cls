global class BigQueryDataSourceConnection extends DataSource.Connection {
    
    // TODO: bring these settings out
    private String projectId = 'salesforce-connect-bigquery';
    private String datasetId = 'gsod';
    private List<String> tableIds = new List<String>();

    private String baseURL = 'https://www.googleapis.com/bigquery/v2';

    private GoogleAuthProvider auth;
    private DataSource.ConnectionParams connectionParams;
    
    global BigQueryDataSourceConnection(DataSource.ConnectionParams connectionParams) {
        this.auth = new GoogleAuthProvider();
        this.connectionParams = connectionParams;
    }

    override global List<DataSource.Table> sync() {
        List<DataSource.Table> tables = new List<DataSource.Table>();
        tableIds.add('gsod_stations');
        tableIds.add('gsod_current');

        if(auth.authorize()) {
            for(String tableId: tableIds) {
                String accessToken = auth.getAccessToken();
                String response = httpGet(baseURL + '/projects/'+ projectId + '/datasets/' + datasetId + '/tables/' + tableId, accessToken);

                Map<String,Object> bqTable = (Map<String,Object>)JSON.deserializeUntyped(response);
                String tableName = tableId;

                List<DataSource.Column> tableColumns = new List<DataSource.Column>();
                tableColumns.add(DataSource.Column.text('ExternalId', 255));
                tableColumns.add(DataSource.Column.url('DisplayUrl'));
                
                List<Object> fieldList = (List<Object>)((Map<String,Object>)bqTable.get('schema')).get('fields');
                for(Object field : fieldList) {
                    // TODO: create different fields according to data type
                    Map<String,Object> fieldMap = (Map<String,Object>) field;
                    String fieldName = (String)fieldMap.get('name');
                    if(fieldName != 'ExternalId' && fieldName != 'DisplayUrl') {
                        tableColumns.add(DataSource.Column.text(fieldName, 255));
                    }
                }
                
                DataSource.Table extTable = DataSource.Table.get(tableName, 'Name', tableColumns);
                tables.add(extTable);
            }
        }

        return tables;
    }

    public List<Map<String,Object>> query(String query, String accessToken) {

        JSONGenerator generator = JSON.createGenerator(false);
        generator.writeStartObject();
        generator.writeStringField('query', query);
        generator.writeBooleanField('useLegacySql', false);
        generator.writeEndObject();
        String requestBody = generator.getAsString();

        String responseBody = httpPost(baseURL + '/projects/'+ projectId + '/queries', requestBody, accessToken);

        Map<String,Object> responseJson = (Map<String,Object>)JSON.deserializeUntyped(responseBody);
        Boolean jobComplete = (Boolean)responseJson.get('jobComplete');

        List<Map<String,Object>> processedRows = new List<Map<String,Object>>();

        if(jobComplete) {
            // Warning: generics madness ahead
            List<Object> fields = (List<Object>)((Map<String,Object>)responseJson.get('schema')).get('fields');
            List<Object> rows = (List<Object>)responseJson.get('rows');

            for(Integer row = 0; row < rows.size(); row++) {
                Map<String, String> processedRow = new Map<String,String>();

                for(Integer field = 0; field < fields.size(); field++) {
                    String fieldName = (String)((Map<String,Object>)fields.get(field)).get('name');
                    String fieldValue = (String)((Map<String,Object>)((List<Object>)((Map<String,Object>)rows.get(row)).get('f')).get(field)).get('v');
                    processedRow.put(fieldName, fieldValue);
                }

                processedRows.add(processedRow);
            }
        }

        return processedRows;
    }

    override global DataSource.TableResult query(DataSource.QueryContext context) {

        //TODO: use the context object to extract informations to be passed to BigQuery

        if(auth.authorize()) {
            String query = buildQuery(context);
            
            return DataSource.TableResult.get(context, query(query, auth.getAccessToken()));
        } else {
            return null;
        }        
    }

    private String buildQuery(DataSource.QueryContext context) {
        String query = 'SELECT ';
        
        // FROM clause
        String columnsList = '';
        for(Integer i = 0; i < context.tableSelection.columnsSelected.size(); i++) {
            columnsList += context.tableSelection.columnsSelected.get(i).columnName;
            if(i < context.tableSelection.columnsSelected.size() - 1) {
                columnsList += ',';
            }
        }

        query += columnsList + ' FROM `' + projectId + '.' + datasetId + '.' + context.tableSelection.tableSelected + '` ';

        // WHERE clause
        String filter = FilterBuilder.build(context.tableSelection.filter);
        if(filter != '()') {
            query += 'WHERE ' + filter + ' ';
        }

        // ORDER clause
        query += 'ORDER BY ';
        for(Integer i = 0; i < context.tableSelection.order.size(); i++) {
            query += context.tableSelection.order.get(i).columnName + ' ';
            switch on context.tableSelection.order.get(i).direction {
                when DataSource.OrderDirection.ASCENDING {
                    query += 'ASC';
                }
                when DataSource.OrderDirection.DESCENDING {
                    query += 'DESC';
                }
            }
            if(i < context.tableSelection.order.size() - 1) {
                query += ', ';
            } else {
                query += ' ';
            }

        }

        query += 'LIMIT 100';
        return query;

    }

    private String httpGet(String endpoint, String accessToken) {
        HttpRequest req = new HttpRequest();
        req.setMethod('GET');
        req.setEndpoint(endpoint);
        req.setHeader('Authorization', 'Bearer ' + accessToken);
        Http http = new Http();
        HttpResponse res = http.send(req);
        return res.getBody();
    }

    private String httpPost(String endpoint, String requestBody, String accessToken) {
        HttpRequest request = new HttpRequest();
        request.setMethod('POST');
        request.setEndpoint(endpoint);
        request.setHeader('Authorization', 'Bearer ' + accessToken);
        request.setBody(requestBody);
        request.setHeader('Content-Type', 'application/json');
        Http http = new Http();
        HttpResponse response = http.send(request);
        return response.getBody();
    }


    //Full text search not available

    override global List<DataSource.TableResult> search(DataSource.SearchContext context) {
        List<DataSource.TableResult> results = new List<DataSource.TableResult>();

        return results;
    }

    //Write capabilities not available; need to investigate Streaming API

    global override List<DataSource.UpsertResult> upsertRows(DataSource.UpsertContext context) {
        List<DataSource.UpsertResult> results = new List<DataSource.UpsertResult>();

        return results;
    }

    //Delete capabilities not available; need to investigate DML

    global override List<DataSource.DeleteResult> deleteRows(DataSource.DeleteContext context) {
        List<DataSource.DeleteResult> results = new List<DataSource.DeleteResult>();

        return results;   
    }

}
