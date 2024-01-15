/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util
import java.util.Locale

private case class TableSection(header: String) {
  var rows: Seq[String] = Seq.empty

  def addRow(row: String): Unit = {
    rows = rows :+ row
  }
}

class SingleColumnConsoleTableBuilder(title: String) {
   private var sections: Seq[TableSection] = Seq.empty

  def addHeader(header: String): SingleColumnConsoleTableBuilder = {
    sections = sections :+ TableSection(header)
    this
  }

  def addRows(rows: String*): SingleColumnConsoleTableBuilder = {
      rows.foreach(row => sections.last.addRow(row))
      this
  }

  def print(): Unit = {
    val padding = 2 + 2 // 2 spaces on either side

    // Determine the max-width so that we can center
    val width = (sections.flatMap { sections =>
        sections.header +: sections.rows
    } :+ title).map(_.length).max + padding

    // scalastyle:off println
    printTitle(title, width)

    sections.foreach { section =>
      // Print the section header
      printSeparatorRow(width)
      printTextRow(section.header.toUpperCase(Locale.ROOT), width, center = true)
      printSeparatorRow(width)

      // Print each row
      section.rows.foreach { row =>
        printTextRow(row, width, center = false)
      }
    }

    printSeparatorRow(width)
    // scalastyle:on println
  }

  private def printTitle(title: String, width: Int): Unit = {
    // scalastyle:off println
    val leftPadding = (width - title.length) / 2
    val rightPadding = width - title.length - leftPadding

    // Print a box around the title
    println(s"+${"=" * width}+")
    println(s"|${" " * leftPadding}$title${" " * rightPadding}|")
    println(s"+${"=" * width}+")
    // scalastyle:on println
  }


  private def printSeparatorRow(width: Int): Unit = {
    // scalastyle:off println
    println(s"|${"-" * width}|")
    // scalastyle:on println
  }

  private def printTextRow(text: String, width: Int, center: Boolean): Unit = {
    // Assume non-centered text
    var leftPadding = 2
    var rightPadding = width - text.length - leftPadding

    if (center) {
      val hasImbalancedSides = (width - text.length) % 2 != 0
      leftPadding = (width - text.length) / 2

      val extraRightPadding = if (hasImbalancedSides) 1 else 0
      rightPadding = leftPadding + extraRightPadding
    }

    // scalastyle:off println
    println(s"|${" " * leftPadding}$text${" " * rightPadding}|")
    // scalastyle:on println
  }
}
