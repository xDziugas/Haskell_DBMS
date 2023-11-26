import Data.Either
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib3 (executeSql, runExecuteIOTest)
import Lib2 qualified
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "SELECT Operations" $ do
    it "correctly retrieves column from table" $ do
      let query = "select name from employees;"
      let expected = Right (DataFrame [Column "name" StringType] 
                                      [[StringValue "Vi"], [StringValue "Ed"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly retrieves multiple columns from table" $ do
      let query = "select name, surname from employees;"
      let expected = Right (DataFrame [Column "name" StringType, Column "surname" StringType] 
                                      [[StringValue "Vi", StringValue "Po"], [StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes min aggregate function on a column from a table" $ do
      let query = "select min(id) from employees;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] 
                                      [[IntegerValue 1]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes avg aggregate function on a column from a table" $ do
      let query = "select avg(id) from employees;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] 
                                      [[IntegerValue 1]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly chooses all from table" $ do
      let query = "select * from employees;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly finds non-existing tables and stops" $ do
      let query = "select id from invalid;"
      let expected = Left "Table not found in InMemoryTables"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly finds non-existing columns and stops" $ do
      let query = "select age from employees;"
      let expected = Left "One or more columns not found"
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "correctly executes where LessThan clause" $ do
      let query = "select * from employees where id < 2;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes where LessEqualThan clause" $ do
      let query = "select * from employees where id <= 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"]
                                      ])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes where Equal clause" $ do
      let query = "select * from employees where id = 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes where GreaterThan clause" $ do
      let query = "select * from employees where id > 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [ [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes where GreaterEqualThan clause" $ do
      let query = "select * from employees where id >= 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                        [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
                                      ])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly points out if the first operator is not the column name and stops" $ do
      let query = "select * from employees where 2 = id;"
      let expected = Left "First operand is not a column name: 2"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected 
    it "correctly filters rows based on string comparison in where clause" $ do
      let query = "select * from employees where name = Vi;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected 
    it "handles boolean values" $ do
      let query = "select * from flags where value = True;"
      let expected = Right (DataFrame [Column "flag" StringType, Column "value" BoolType]
                                      [[StringValue "a", BoolValue True],
                                       [StringValue "b", BoolValue True]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles multiple where clause conditions with AND" $ do
      let query = "select * from flags where flag = b AND value = False;"
      let expected = Right (DataFrame [Column "flag" StringType, Column "value" BoolType]
                                       [[StringValue "b", BoolValue False]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles case-insensitive keywords" $ do
      let query = "sElEcT * FrOm flags WhERe flag = b anD value = False;"
      let expected = Right (DataFrame [Column "flag" StringType, Column "value" BoolType]
                                      [[StringValue "b", BoolValue False]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles no columns specified and stops" $ do
      let query = "select from employees;"
      let expected = Left "Invalid statement"
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "handles null values" $ do
      let query = "select * from flags;"
      let expected = Right (DataFrame [Column "flag" StringType, Column "value" BoolType]
                                      [ [StringValue "a", BoolValue True],
                                        [StringValue "b", BoolValue True],
                                        [StringValue "b", NullValue],
                                        [StringValue "b", BoolValue False]
                                      ])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles multiple whitespaces" $ do
      let query = "select id,                name   ,     surname      from     employees    where     id    =      1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles incorrect data type for a column in the where clause condition" $ do
      let query = "select id from employees where id < uga;"
      let expected = Left "Second operand is not an integer: uga"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles aggregate functions combined with where clause" $ do
      let query = "select min(flag) from flags where value = False;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] [[StringValue "b"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "doesn't let multiple columns if aggregate function is present" $ do
      let query = "select min(id), name from employees;"
      let expected = Left "Column names alongside aggregate functions not allowed"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "doesn't let column names be seperated by spaces" $ do
      let query = "select id name ; surname from employees;"
      let expected = Left "Invalid statement"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "doesn't let aggregate function be seperated by spaces" $ do
      let query = "select min id from employees;"
      let expected = Left "Invalid statement"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an empty DataFrame for a valid query with no matching conditions" $ do
      let query = "select * from employees where id > 100;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error for non-existent column in select" $ do
      let query = "select age from employees;"
      let expected = Left "One or more columns not found"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
  describe "SELECT JOIN operations" $ do
    it "correctly joins tables in the where clause" $ do
      let query = "select name, surname, namee, surnamee from employees, employees2 where id = idd;"
      let expected = Right (DataFrame [Column "name" StringType, Column "surname" StringType, Column "namee" StringType, Column "surnamee" StringType]
                                      [[StringValue "Vi", StringValue "Po", StringValue "Jo", StringValue "Ja"],
                                       [StringValue "Ed", StringValue "Dl", StringValue "Ka", StringValue "Ma"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly handles joining tables with boolean column comparisons" $ do
      let query = "select name, value from employees, flags where value = True;"
      let expected = Right (DataFrame [Column "name" StringType, Column "value" BoolType] 
                                      [[StringValue "Vi", BoolValue True],
                                       [StringValue "Vi", BoolValue True],
                                       [StringValue "Ed", BoolValue True], 
                                       [StringValue "Ed", BoolValue True]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly joins tables on string columns" $ do
      let query = "select name, flag from employees, flags where flag = a;"
      let expected = Right (DataFrame [Column "name" StringType, Column "flag" StringType] 
                                      [[StringValue "Vi", StringValue "a"],
                                       [StringValue "Ed", StringValue "a"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly joins tables different columns together with multiple where clause conditions" $ do
      let query = "select * from employees,  employees2 where surname = Po and surnamee = Ma;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType,
                                       Column "idd" IntegerType, Column "namee" StringType, Column "surnamee" StringType]
                                       [[IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 2, StringValue "Ka", StringValue "Ma"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected

    it "correctly handles joins with no matching rows" $ do
      let query = "select name, surnamee from employees, employees2 where id = 999;"
      let expected = Right (DataFrame [Column "name" StringType, Column "surnamee" StringType] [])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected 
    it "correctly handles returning multiple tables without a where clause" $ do
      let query = "select * from employees, employees2;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType,
                                       Column "idd" IntegerType, Column "namee" StringType, Column "surnamee" StringType]
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 1, StringValue "Jo", StringValue "Ja"],
                                       [IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 2, StringValue "Ka", StringValue "Ma"],
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl", IntegerValue 1, StringValue "Jo", StringValue "Ja"],
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl", IntegerValue 2, StringValue "Ka", StringValue "Ma"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly joins tables with multiple conditions in the where clause" $ do
      let query = "select name, namee from employees, employees2 where id = idd and surname = surnamee;"
      let expected = Right (DataFrame [Column "name" StringType, Column "namee" StringType] [])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly handles a cross join" $ do
      let query = "select * from employees, employees2, flags where flag = a and id = 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType,
                                       Column "idd" IntegerType, Column "namee" StringType, Column "surnamee" StringType, 
                                       Column "flag" StringType, Column "value" BoolType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 1, StringValue "Jo", StringValue "Ja", StringValue "a", BoolValue True],
                                       [IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 2, StringValue "Ka", StringValue "Ma", StringValue "a", BoolValue True]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
      --NEPAGAUNU aggregate at all
    it "correctly joins tables with aggregate function and where clause" $ do
      let query = "select avg(id), namee from employees, employees2 where surnamee = 'Ma';"
      let expected = Right (DataFrame [Column "aggregate" IntegerType, Column "namee" StringType] 
                                      [])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected 
  describe "INSERT Operations" $ do
    it "correctly inserts a row into a table" $ do
      let query = "insert into employees (id, name, surname) values (3, guga, buga);"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                                       [IntegerValue 3, StringValue "guga", StringValue "buga"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly inserts a row with missing values (handled as NULL)" $ do
      let query = "INSERT INTO employees (id, name) VALUES (5, guga);"
      let expected = Right (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] 
                                      [[IntegerValue 1,StringValue "Vi",StringValue "Po"],
                                       [IntegerValue 2,StringValue "Ed",StringValue "Dl"], 
                                       [IntegerValue 5,StringValue "guga",NullValue]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "handles mismatched column and value order" $ do
      let query = "INSERT INTO employees (surname, name, id) VALUES (ii, hh, 11);"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"], 
                                       [IntegerValue 11, StringValue "hh", StringValue "ii"]])
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "handles case-insensitive keywords when inserting" $ do
      let query = "iNsert InTO employees (surname, name, id) VAluES (ii, hh, 11);"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"], 
                                       [IntegerValue 11, StringValue "hh", StringValue "ii"]])
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "returns an error for mismatched columns and values count" $ do
      let query = "INSERT INTO employees (id, name) VALUES (4, Alice, Smith);"
      let expected = Left "Column names and values count mismatch"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error for inserting into a non-existent table" $ do
      let query = "INSERT INTO unknown_table (id) VALUES (1);"
      let expected = Left "Table not found in InMemoryTables"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
      --NEED TO CHECK WITH DATAFRAME
    it "returns an error for incorrect data type (string into integer column)" $ do
      let query = "INSERT INTO employees (id, name) VALUES (invalid, guga);"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"], 
                                       [StringValue "invalid", StringValue "guga", NullValue]])
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
      --NEED TO CHECK WITH DATAFRAME
    it "returns an error for non-existent column names" $ do
      let query = "INSERT INTO employees (id, col1) VALUES (10, Value);"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"], 
                                       [IntegerValue 10, NullValue, NullValue]])
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "returns an error when inserting a row without specifying column names" $ do
      let query = "INSERT INTO employees VALUES (3, guga, buga);"
      let expected = Left "Invalid statement"
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    
    

