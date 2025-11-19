package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import java.util.List;

final class UnparseUtils {
    private UnparseUtils() {}

    static void unparseTableSchema(
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlWatermark watermark,
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        if (columnList.isEmpty() && tableConstraints.isEmpty() && watermark == null) {
            return;
        }
        SqlUnparseUtils.unparseTableSchema(
                writer, leftPrec, rightPrec, columnList, tableConstraints, watermark);
    }

    static void unparseDistribution(
            SqlDistribution distribution, SqlWriter writer, int leftPrec, int rightPrec) {
        if (distribution == null) {
            return;
        }
        writer.newlineAndIndent();
        distribution.unparse(writer, leftPrec, rightPrec);
    }

    static void unparsePartitionKeyList(
            SqlNodeList partitionKeyList, SqlWriter writer, int leftPrec, int rightPrec) {
        if (partitionKeyList.isEmpty()) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("PARTITIONED BY");
        SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
        partitionKeyList.unparse(writer, leftPrec, rightPrec);
        writer.endList(partitionedByFrame);
    }

    static void unparseFreshness(
            SqlIntervalLiteral freshness, SqlWriter writer, int leftPrec, int rightPrec) {
        if (freshness == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("FRESHNESS");
        writer.keyword("=");
        freshness.unparse(writer, leftPrec, rightPrec);
    }

    static void unparseRefreshMode(
            SqlRefreshMode refreshMode, SqlWriter writer, int leftPrec, int rightPrec) {
        if (refreshMode == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("REFRESH_MODE");
        writer.keyword("=");
        writer.keyword(refreshMode.name());
    }

    static void unparseComment(
            SqlCharStringLiteral comment, SqlWriter writer, int leftPrec, int rightPrec) {
        if (comment == null) {
            return;
        }
        writer.newlineAndIndent();
        writer.keyword("COMMENT");
        comment.unparse(writer, leftPrec, rightPrec);
    }

    static void unparseSetProperties(
            SqlNodeList propertyList, SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SET");
        unparsePropertiesBlock(propertyList, writer, leftPrec, rightPrec);
    }

    static void unparseResetProperties(
            SqlNodeList propertyList, SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RESET");
        unparsePropertiesBlock(propertyList, writer, leftPrec, rightPrec);
    }

    private static void unparsePropertiesBlock(
            SqlNodeList propertyList, SqlWriter writer, int leftPrec, int rightPrec) {
        SqlWriter.Frame withFrame = writer.startList("(", ")");
        for (SqlNode property : propertyList) {
            SqlUnparseUtils.printIndent(writer);
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.endList(withFrame);
    }
}
