module InMemoryTables
  ( tableEmployees,
    tableInvalid1,
    tableInvalid2,
    tableLongStrings,
    tableWithNulls,
    database,
    TableName,
  )
where

import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))

type TableName = String

tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
        [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
      ]
  )

tableEmployees2 :: (TableName, DataFrame)
tableEmployees2 =
  ( "employees2",
    DataFrame
      [Column "idd" IntegerType, Column "namee" StringType, Column "surnamee" StringType]
      [ [IntegerValue 1, StringValue "Jo", StringValue "Ja"],
        [IntegerValue 2, StringValue "Ka", StringValue "Ma"]
      ]
  )

tableInvalid1 :: (TableName, DataFrame)
tableInvalid1 =
  ( "invalid1",
    DataFrame
      [Column "id" IntegerType]
      [ [StringValue "1"]
      ]
  )

tableInvalid2 :: (TableName, DataFrame)
tableInvalid2 =
  ( "invalid2",
    DataFrame
      [Column "id" IntegerType, Column "text" StringType]
      [ [IntegerValue 1, NullValue],
        [IntegerValue 1]
      ]
  )

longString :: Value
longString =
  StringValue $
    unlines
      [ "Lorem ipsum dolor sit amet, mei cu vidisse pertinax repudiandae, pri in velit postulant vituperatoribus.",
        "Est aperiri dolores phaedrum cu, sea dicit evertitur no. No mei euismod dolorem conceptam, ius ne paulo suavitate.",
        "Vim no feugait erroribus neglegentur, cu sed causae aeterno liberavisse,",
        "his facer tantas neglegentur in. Soleat phaedrum pri ad, te velit maiestatis has, sumo erat iriure in mea.",
        "Numquam civibus qui ei, eu has molestiae voluptatibus."
      ]

tableLongStrings :: (TableName, DataFrame)
tableLongStrings =
  ( "long_strings",
    DataFrame
      [Column "text1" StringType, Column "text2" StringType]
      [ [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString]
      ]
  )

tableWithNulls :: (TableName, DataFrame)
tableWithNulls =
  ( "flags",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType]
      [ [StringValue "a", BoolValue True],
        [StringValue "b", BoolValue True],
        [StringValue "b", NullValue],
        [StringValue "b", BoolValue False]
      ]
  )


tableTestAggregateJoin :: (TableName, DataFrame)
tableTestAggregateJoin =
  ( "test_aggregate_join",
    DataFrame
      [Column "id" IntegerType, Column "related_id" IntegerType, Column "data" StringType]
      [ [IntegerValue 20, IntegerValue 1, StringValue "Data1"],
        [IntegerValue 5, IntegerValue 2, StringValue "Data2"],
        [IntegerValue 50, IntegerValue 3, StringValue "Data3"]
      ]
  )

tableTasks :: (TableName, DataFrame)
tableTasks =
  ( "tasks",
    DataFrame
      [Column "task_id" IntegerType, Column "description" StringType, Column "is_completed" BoolType]
      [  [IntegerValue 601, StringValue "Setup project environment", BoolValue False],
         [IntegerValue 602, StringValue "Complete initial documentation", BoolValue True],
         [IntegerValue 603, StringValue "Implement core features", BoolValue False],
         [IntegerValue 604, StringValue "Conduct code review", BoolValue False],
         [IntegerValue 605, StringValue "Prepare for deployment", BoolValue True]
      ]
  )


database :: [(TableName, DataFrame)]
database = [tableEmployees, tableEmployees2, tableTasks, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls, tableTestAggregateJoin]

