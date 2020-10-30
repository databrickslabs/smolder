package com.databricks.smolder.sql

import com.databricks.smolder.Message
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class ParseHL7Message(child: Expression)
    extends UnaryExpression {

  override def dataType: DataType = Message.schema
  override def nullSafeEval(input: Any): Any = Message(input.asInstanceOf[UTF8String]).toInternalRow()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      c => {
        s"""
         |${ev.value} =
         |com.databricks.smolder.Message.apply($c).toInternalRow();
       """.stripMargin
      }
    )
  }
}
