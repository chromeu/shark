/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2

import java.sql.Timestamp

import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryBinary
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._


import org.scalatest.FunSuite

import shark.memstore2.column._


class ColumnIteratorSuite extends FunSuite {

  val PARALLEL_MODE = true

  test("non-null void column") {
    val builder = new VoidColumnBuilder
    builder.initialize(5)
    builder.append(null, null)
    builder.appendNull()
    builder.append(null, null)
    val buffer = builder.build

    val iter = new VoidColumnIterator
    iter.initialize(buffer)
    assert(iter.next == NullWritable.get())
    assert(iter.next == NullWritable.get())
    assert(iter.next == NullWritable.get())
  }

  test("non-null boolean column") {
    val builder = new BooleanColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Boolean](true, false, true, true, true),
      builder,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator])
    assert(builder.stats.min === false)
    assert(builder.stats.max === true)
  }

  test("non-null byte column") {
    val builder = new ByteColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Byte](1.toByte, 2.toByte, 15.toByte, 55.toByte, 0.toByte, 40.toByte),
      builder,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator])
    assert(builder.stats.min === 0.toByte)
    assert(builder.stats.max === 55.toByte)
  }

  test("non-null short column") {
    val builder = new ShortColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, 355.toShort, 0.toShort, 40.toShort),
      builder,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator])
    assert(builder.stats.min === -15.toShort)
    assert(builder.stats.max === 355.toShort)
  }

  test("non-null int column") {
    val builder = new IntColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Integer](0, 1, 2, 5, 134, -12, 1, 0, 99, 1),
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator])
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)
  }

  test("non-null long column") {
    val builder = new LongColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Long](1L, -345345L, 15L, 0L, 23445456L),
      builder,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator])
    assert(builder.stats.min === -345345L)
    assert(builder.stats.max === 23445456L)
  }

  test("non-null float column") {
    val builder = new FloatColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Float](1.1.toFloat, -2.5.toFloat, 20000.toFloat, 0.toFloat, 15.0.toFloat),
      builder,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator])
    assert(builder.stats.min === -2.5.toFloat)
    assert(builder.stats.max === 20000.toFloat)
  }

  test("non-null double column") {
    val builder = new DoubleColumnBuilder
    testNonNullColumnIterator(
      Array[java.lang.Double](1.1, 2.2, -2.5, 20000, 0, 15.0),
      builder,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator])
    assert(builder.stats.min === -2.5)
    assert(builder.stats.max === 20000)
  }

  test("non-null string column") {
    val builder = new StringColumnBuilder
    testNonNullColumnIterator(
      Array[Text](new Text("a"), new Text(""), new Text("b"), new Text("Abcdz")),
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[StringColumnIterator],
      (a, b) => (a.equals(b.toString))
    )
    assert(builder.stats.min.toString === "")
    assert(builder.stats.max.toString === "b")
  }

  test("non-null timestamp column") {
    val ts1 = new java.sql.Timestamp(0)
    val ts2 = new java.sql.Timestamp(500)
    ts2.setNanos(400)
    val ts3 = new java.sql.Timestamp(1362561610000L)

    val builder = new TimestampColumnBuilder
    testNonNullColumnIterator(
      Array(ts1, ts2, ts3),
      builder,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator],
      (a, b) => (a.equals(b))
    )
    assert(builder.stats.min.equals(ts1))
    assert(builder.stats.max.equals(ts3))
  }

  test("non-null binary column") {
    val rowOI = LazyPrimitiveObjectInspectorFactory.LAZY_BINARY_OBJECT_INSPECTOR
    val binary1 = LazyFactory.createLazyPrimitiveClass(rowOI).asInstanceOf[LazyBinary]
    val ref1 = new ByteArrayRef
    val data = Array[Byte](0, 1, 2)
    ref1.setData(data)
    binary1.init(ref1, 0, 3)

    val builder = new BinaryColumnBuilder
    testNonNullColumnIterator(
      Array[LazyBinary](binary1),
      builder,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
      classOf[BinaryColumnIterator],
      compareBinary)
    assert(builder.stats == null)

    def compareBinary(x: Object, y: Object): Boolean = {
      val xdata = x.asInstanceOf[ByteArrayRef].getData
      val ydata = y.asInstanceOf[LazyBinary].getWritableObject().getBytes()
      java.util.Arrays.equals(xdata, ydata)
    }
  }

  def testNonNullColumnIterator[T](
    testData: Array[_ <: Object],
    builder: ColumnBuilder[T],
    writableOi: AbstractPrimitiveWritableObjectInspector,
    iteratorClass: Class[_ <: ColumnIterator],
    compareFunc: (Object, Object) => Boolean = (a, b) => a == b) {

    builder.initialize(5)
    testData.foreach(x => builder.append(x.asInstanceOf[T]))
    val buffer = builder.build
    val columnType = buffer.getLong().toInt
    buffer.rewind()

    assert(ColumnIterator.getIteratorClass(columnType) === iteratorClass)

    def executeOneTest() {
      val iter = iteratorClass.newInstance.asInstanceOf[ColumnIterator]
      iter.initialize(buffer)
      (0 until testData.size).foreach { i =>
        val expected = testData(i)
        val reality = writableOi.getPrimitiveJavaObject(iter.next)
        assert((expected == null && reality == null) || compareFunc(reality, expected),
          "at position " + i + " expected " + expected + ", but saw " + reality)
      }
    }

    if (PARALLEL_MODE) {
      // parallelize to test concurrency
      (1 to 10).par.foreach { parallelIndex => executeOneTest() }
    } else {
      executeOneTest()
    }
  }
}
