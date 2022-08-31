package com.alibaba.datax.plugin.writer.starrockswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum StarRocksWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("StarRocksWriter-00", "运行时异常"),
    ILLEGAL_VALUE("StarRocksWriter-01", "您填写的参数值不合法."),
    CONFIG_INVALID_EXCEPTION("StarRocksWriter-02", "您的参数配置错误."),
    SECURITY_NOT_ENOUGH("TxtFileWriter-03", "您缺少权限执行相应的文件写入操作.");



    private final String code;
    private final String description;

    private StarRocksWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
