package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import dm.jdbc.driver.DmdbBlob;
import dm.jdbc.driver.DmdbClob;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * Dameng.
 */
public class DamengRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public DamengRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
                return val -> val;
            case TINYINT:
                return val -> {
                    if (val instanceof Byte) {
                        return val;
                    } else if (val instanceof Short) {
                        return ((Short) val).byteValue();
                    } else {
                        return ((Integer) val).byteValue();
                    }
                };
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> {
                    // support text type
                    if (val instanceof DmdbClob) {
                        try {
                            return StringData.fromString(
                                    inputStream2String(((DmdbClob) val).getAsciiStream()));
                        } catch (Exception e) {
                            throw new UnsupportedOperationException(
                                    "failed to get length from text");
                        }
                    } else if (val instanceof DmdbBlob) {
                        try {
                            return StringData.fromString(
                                    inputStream2String(((DmdbBlob) val).getBinaryStream()));
                        } catch (Exception e) {
                            throw new UnsupportedOperationException(
                                    "failed to get length from text");
                        }
                    } else {
                        return StringData.fromString((String) val);
                    }
                };
            case BINARY:
            case VARBINARY:
                return val ->
                        val instanceof DmdbBlob
                                ? ((DmdbBlob) val).getBytes(1, (int) ((DmdbBlob) val).length())
                                : val.toString().getBytes();
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                return super.createInternalConverter(type);
        }
    }

    /**
     * get String from inputStream.
     *
     * @param input inputStream
     * @return String value
     * @throws IOException convert exception
     */
    private static String inputStream2String(InputStream input) throws IOException {
        StringBuilder stringBuffer = new StringBuilder();
        byte[] byt = new byte[1024];
        for (int i; (i = input.read(byt)) != -1; ) {
            stringBuffer.append(new String(byt, 0, i));
        }
        return stringBuffer.toString();
    }

    @Override
    public String converterName() {
        return "Dameng";
    }
}
