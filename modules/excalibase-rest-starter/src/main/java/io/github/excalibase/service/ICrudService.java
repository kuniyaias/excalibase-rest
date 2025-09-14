package io.github.excalibase.service;

import io.github.excalibase.model.TableInfo;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

/**
 * Interface for CRUD operations providing database abstraction layer.
 * This interface matches the actual CrudService method signatures for multi-database support.
 */
public interface ICrudService {

    Map<String, Object> createRecord(String tableName, Map<String, Object> data);

    List<Map<String, Object>> createBulkRecords(String tableName, List<Map<String, Object>> dataList);

    Map<String, Object> updateRecord(String tableName, String id, Map<String, Object> data);

    List<Map<String, Object>> updateBulkRecords(String tableName, List<Map<String, Object>> updateList);

    boolean deleteRecord(String tableName, String id);

    Map<String, Object> deleteRecordsByFilters(String tableName, MultiValueMap<String, String> filters, FilterService filterService);

    Map<String, Object> updateRecordsByFilters(String tableName, MultiValueMap<String, String> filters,
                                             Map<String, Object> updateData, FilterService filterService);
}