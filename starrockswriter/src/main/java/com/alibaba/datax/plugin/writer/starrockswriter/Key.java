package com.alibaba.datax.plugin.writer.starrockswriter;

public class Key {
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    public static final String PRINT = "print";

    public static final String PATH = "path";

    public static final String FILE_NAME = "fileName";

    public static final String RECORD_NUM_BEFORE_SLEEP = "recordNumBeforeSleep";

    public static final String SLEEP_TIME = "sleepTime";

    public static final String CSV = "csv";

    public static final String JSON = "json";

    public static final String RESULT_FAILED = "Fail";
    public static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    public static final String LAEBL_STATE_VISIBLE = "VISIBLE";
    public static final String LAEBL_STATE_COMMITTED = "COMMITTED";
    public static final String RESULT_LABEL_PREPARE = "PREPARE";
    public static final String RESULT_LABEL_ABORTED = "ABORTED";
    public static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String COLUMNS = "columns";
    public static final String JSON_COLUMNS = "json_columns";
    public static final String ADDRESS = "address";
    public static final String LABEL_PREFIX = "label_prefix";
    public static final String DEFAULT_LABEL_PREFIX = "datax_";
    public static final String ROW_DELIMITER = "row_delimiter";
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final String FORMAT = "format";
    public static final String DEFAULT_FORMAT = CSV;

    public static final String JSON_PATHS = "jsonpaths";

    public static final int FLUSH_BYTE_SIZE = 100 * 1024 * 1024;
    public static final int FLUSH_COUNT = 10000;

    public static final int MAX_RETRY = 3;


}
