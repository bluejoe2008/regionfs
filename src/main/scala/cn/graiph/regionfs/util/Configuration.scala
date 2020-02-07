/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.graiph.regionfs.util

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  * Created by bluejoe on 2019/7/23.
  */
trait Configuration {
  def getRaw(name: String): Option[String]
}

/**
  * Created by bluejoe on 2018/11/3.
  */
class ConfigurationEx(conf: Configuration) extends Logging {

  trait AnyValue {
    protected def safeConvert[T](convert: (String) => T)(implicit m: Manifest[T]): T;

    def asInt: Int = safeConvert(_.toInt);

    def asLong: Long = safeConvert(_.toLong);

    def asString: String = safeConvert(_.toString);

    def asBoolean: Boolean = safeConvert(_.toBoolean);

    def asFile(baseDir: File): File = safeConvert { x =>
      val file = new File(x)
      if (file.isAbsolute)
        file
      else
        new File(baseDir, x)
    }
  }

  class ConfigValue(key: String, maybeValue: Option[String]) extends AnyValue {
    def safeConvert[T](convert: (String) => T)(implicit m: Manifest[T]): T = {
      if (!maybeValue.isDefined)
        throw new ArgumentRequiredException(key)

      val value = maybeValue.get;
      try {
        convert(value)
      }
      catch {
        case e: java.lang.IllegalArgumentException =>
          throw new WrongArgumentException(key, value, m.runtimeClass)
      }
    }

    def withDefault(defaultValue: Any): AnyValue = new ConfigValue(key, maybeValue) {
      override def safeConvert[T](convert: (String) => T)(implicit m: Manifest[T]): T = {
        if (maybeValue.isEmpty) {
          logger.debug(s"no value set for $key, using default: $defaultValue")
          defaultValue.asInstanceOf[T]
        }
        else {
          super.safeConvert(convert)
        }
      }
    }
  }

  def this(props: Properties) = {
    this(new Configuration {
      override def getRaw(name: String): Option[String] =
        if (props.containsKey(name))
          Some(props.getProperty(name))
        else
          None
    });
  }

  def this(propsFile: File) = {
    this({
      val props = new Properties()
      val fis = new FileInputStream(propsFile)
      props.load(fis)
      fis.close()
      props
    });
  }

  def get(key: String): ConfigValue = {
    new ConfigValue(key, conf.getRaw(key))
  }
}

class ArgumentRequiredException(key: String) extends
  RuntimeException(s"argument required: $key") {

}

class WrongArgumentException(key: String, value: String, clazz: Class[_]) extends
  RuntimeException(s"wrong argument: $key, value=$value, expected: $clazz") {

}

object ConfigUtils {
  implicit def config2Ex(conf: Configuration) = new ConfigurationEx(conf)
}
