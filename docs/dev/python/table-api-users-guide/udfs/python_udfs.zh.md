---
title: "普通自定义函数（UDF）"
nav-id: general_python_udf
nav-parent_id: python_udf
nav-pos: 20
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

用户自定义函数是重要的功能，因为它们极大地扩展了Python Table API程序的表达能力。

**注意:** 要执行Python用户自定义函数，客户端和集群端都需要安装Python版本(3.5、3.6、3.7 或 3.8)，并安装PyFlink。

* This will be replaced by the TOC
{:toc}

## 标量函数（ScalarFunction）

PyFlink支持在Python Table API程序中使用Python标量函数。 如果要定义Python标量函数，
可以继承`pyflink.table.udf`中的基类ScalarFunction，并实现eval方法。
Python标量函数的行为由名为`eval`的方法定义，eval方法支持可变长参数，例如`eval(* args)`。

以下示例显示了如何定义自己的Python哈希函数、如何在TableEnvironment中注册它以及如何在作业中使用它。

{% highlight python %}
from pyflink.table.expressions import call 

class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# 在Python Table API中使用Python自定义函数
my_table.select(my_table.string, my_table.bigint, hash_code(my_table.bigint), call(hash_code, my_table.bigint))

# 在SQL API中使用Python自定义函数
table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

除此之外，还支持在Python Table API程序中使用Java / Scala标量函数。

{% highlight python %}
'''
Java code:

// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中可以加载到。
public class HashCode extends ScalarFunction {
  private int factor = 12;

  public int eval(String s) {
      return s.hashCode() * factor;
  }
}
'''
from pyflink.table.expressions import call

table_env = BatchTableEnvironment.create(env)

# 注册Java函数
table_env.create_java_temporary_function("hash_code", "my.java.function.HashCode")

# 在Python Table API中使用Java函数
my_table.select(call('hash_code', my_table.string))

# 在SQL API中使用Java函数
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
{% endhighlight %}

除了扩展基类`ScalarFunction`之外，还支持多种方式来定义Python标量函数。
以下示例显示了多种定义Python标量函数的方式。该函数需要两个类型为bigint的参数作为输入参数，并返回它们的总和作为结果。

{% highlight python %}
# 方式一：扩展基类`ScalarFunction`
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add = udf(Add(), result_type=DataTypes.BIGINT())

# 方式二：普通Python函数
@udf(result_type=DataTypes.BIGINT())
def add(i, j):
  return i + j

# 方式三：lambda函数
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())

# 方式四：callable函数
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add = udf(CallableAdd(), result_type=DataTypes.BIGINT())

# 方式五：partial函数
def partial_add(i, j, k):
  return i + j + k

add = udf(functools.partial(partial_add, k=1), result_type=DataTypes.BIGINT())

# 注册Python自定义函数
table_env.create_temporary_function("add", add)
# 在Python Table API中使用Python自定义函数
my_table.select("add(a, b)")

# 也可以在Python Table API中直接使用Python自定义函数
my_table.select(add(my_table.a, my_table.b))
{% endhighlight %}

## 表值函数
与Python用户自定义标量函数类似，Python用户自定义表值函数以零个，一个或者多个列作为输入参数。但是，与标量函数不同的是，表值函数可以返回
任意数量的行作为输出而不是单个值。Python用户自定义表值函数的返回类型可以是Iterable，Iterator或generator类型。

以下示例说明了如何定义自己的Python自定义表值函数，将其注册到TableEnvironment中，并在作业中使用它。

{% highlight python %}
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# 注册Python表值函数
split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# 在Python Table API中使用Python表值函数
my_table.join_lateral(split(my_table.a).alias("word, length"))
my_table.left_outer_join_lateral(split(my_table.a).alias("word, length"))

# 在SQL API中使用Python表值函数
table_env.create_temporary_function("split", udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()]))
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}

除此之外，还支持在Python Table API程序中使用Java / Scala表值函数。
{% highlight python %}
'''
Java code:

// 类型"Tuple2 <String，Integer>"代表，表值函数的输出类型为（String，Integer）。
// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中加载到。
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}
'''
from pyflink.table.expressions import call

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# 注册java自定义函数。
table_env.create_java_temporary_function("split", "my.java.function.Split")

# 在Python Table API中使用表值函数。 "alias"指定表的字段名称。
my_table.join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))
my_table.left_outer_join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))

# 注册python函数。

# 在SQL中将table函数与LATERAL和TABLE关键字一起使用。
# CROSS JOIN表值函数（等效于Table API中的"join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN一个表值函数（等同于Table API中的"left_outer_join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}

像Python标量函数一样，您可以使用上述五种方式来定义Python表值函数。

<span class="label label-info">注意</span> 唯一的区别是，Python表值函数的返回类型必须是iterable（可迭代子类）, iterator（迭代器） or generator（生成器）。

{% highlight python %}
# 方式一：生成器函数
@udtf(result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

# 方式二：返回迭代器
@udtf(result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

# 方式三：返回可迭代子类
@udtf(result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result
{% endhighlight %}
