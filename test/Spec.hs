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
      let query = "select avg(id) from test_aggregate_join;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] 
                                      [[IntegerValue 25]])
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
    it "returns an error when trying to apply aggergate function to not an int" $ do
      let query = "select min(flag) from flags;"
      let expected = Left "Data types are incorrect"
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
    it "returns an error if a given tables column count doesn't match rows count" $ do
      let query = "select * from invalid2;"
      let expected = Left "Row lengths do not match the number of columns"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error if a given tables column count doesn't match rows count" $ do
      let query = "select * from invalid2;"
      let expected = Left "Row lengths do not match the number of columns"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error if a given tables column data type doesn't match rows data type" $ do
      let query = "select * from invalid1;"
      let expected = Left "Data types are incorrect"
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
    it "correctly applies min aggregate function in a join operation" $ do
      let query = "select min(id) from test_aggregate_join, employees2 where related_id = idd;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] 
                                      [[IntegerValue 5]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly applies avg aggregate function in a join operation" $ do
      let query = "select avg(id) from test_aggregate_join, employees2 where related_id = idd;"
      let expected = Right (DataFrame [Column "aggregate" IntegerType] 
                                      [[IntegerValue 12]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error if it found one or more tables inside the join which dont exist" $ do
      let query = "select * from employees, nonExistent;"
      let expected = Left "Table not found in InMemoryTables"
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
    it "returns an error for inserting into a non-existent table" $ do
      let query = "INSERT INTO unknown_table (id) VALUES (1);"
      let expected = Left "Table not found in InMemoryTables"
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "returns an error for incorrect data type (string into integer column)" $ do
      let query = "INSERT INTO employees (id, name) VALUES (invalid, guga);"
      let expected = Left "Type mismatch for column: id"
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "returns an error for non-existent column names" $ do
      let query = "INSERT INTO employees (id, col1) VALUES (10, Value);"
      let expected = Left "One or more specified columns do not exist in the table."
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    it "returns an error when inserting a row without specifying column names" $ do
      let query = "INSERT INTO employees VALUES (3, guga, buga);"
      let expected = Left "Invalid statement"
      result <- runExecuteIOTest $ executeSql query
      result `shouldBe` expected
    describe "NOW Operations" $ do
      it "correctly displays the time" $ do
        let query = "select now() from employees;"
        let expected = Right (DataFrame [Column "current_time" StringType] [[StringValue "2000-01-01 12:00:00"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
      it "correctly displays the time using case-sensitive letters" $ do
        let query = "select nOw() from employees;"
        let expected = Right (DataFrame [Column "current_time" StringType] [[StringValue "2000-01-01 12:00:00"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
      it "correctly handles the time with other columns" $ do
        let query = "select now(), id from employees;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "current_time" StringType] 
                                        [[IntegerValue 1, StringValue "2000-01-01 12:00:00 UTC"], 
                                         [IntegerValue 2, StringValue "2000-01-01 12:00:00 UTC"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
  describe "UPDATE operations" $ do
    it "correctly updates the table when the condition is met" $ do
      let query = "update employees set id = 2 where surname = \"Po\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly handles whitespace" $ do
      let query = "update employees set id = 2 where surname = \"Po\";   "
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly updates the table when the condition is met using case-sensitive letters" $ do
      let query = "uPDatE employees set id = 2 where surname = \"Po\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 2, StringValue "Vi", StringValue "Po"], 
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "leaves the table unchanged when the condition is not met" $ do
      let query = "update employees set id = 2 where surname = \"Nonexistent\";"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "updates multiple rows when the condition is met for each" $ do
        let query = "update employees set id = 2 where id > 1;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                        [[IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                         [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles updates with multiple set conditions" $ do
        let query = "update employees set id = 2, name = \"Updated\" where id = 1;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                        [[IntegerValue 2, StringValue "Updated", StringValue "Po"],
                                         [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "correctly updates boolean values" $ do
        let query = "update tasks set task_id = 601 where is_completed = False;"
        let expected = Right (DataFrame [Column "task_id" IntegerType, Column "description" StringType, Column "is_completed" BoolType]
                                        [[IntegerValue 601, StringValue "Setup project environment", BoolValue False],
                                         [IntegerValue 602, StringValue "Complete initial documentation", BoolValue True],
                                         [IntegerValue 601, StringValue "Implement core features", BoolValue False],
                                         [IntegerValue 601, StringValue "Conduct code review", BoolValue False], 
                                         [IntegerValue 605, StringValue "Prepare for deployment", BoolValue True]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles mismatched data types in WHERE clause" $ do
        let query = "update employees set id = \"a\" where surname = \"Po\";"
        let expected = Left "Data types are incorrect"
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles non-existent table in query" $ do
        let query = "update non_existant_table set id = 1 where surname = \"Po\";"
        let expected = Left "Table not found in InMemoryTables"
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles invalid column name in query" $ do
        let query = "update employees set ids = 3 where id = 1;"
        let expected = Left "Column not found: ids"
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
  describe "DELETE operations" $ do
    it "correctly removes record from table" $ do
        let query = "delete from employees where id = 2;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles extra whitespace and case-sensitive letters" $ do
        let query = "DelETE from employees   where id = 2 ;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                      [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles wrong delete usage" $ do
        let query = "elete from employees   where id = 2 ;"
        let expected = Left "Invalid statement"
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "leaves the table unchanged when a condition is not met" $ do
        let query = "delete from employees where id = 5;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                        [[IntegerValue 1, StringValue "Vi", StringValue "Po"], 
                                         [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "deletes records based on a condition" $ do
        let query = "delete from employees where id > 0;"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles a delete statement with multiple conditions" $ do
        let query = "delete from employees where id = 1 and surname = \"Po\";"
        let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] 
                                        [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "handles a delete statement with boolean values" $ do
        let query = "delete from tasks where is_completed = False;"
        let expected = Right (DataFrame [Column "task_id" IntegerType, Column "description" StringType, Column "is_completed" BoolType]
                                        [[IntegerValue 602, StringValue "Complete initial documentation", BoolValue True], 
                                         [IntegerValue 605, StringValue "Prepare for deployment", BoolValue True]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    it "deletes records based on a complex condition" $ do
        let query = "delete from tasks where task_id > 602 and is_completed = False;"
        let expected = Right (DataFrame [Column "task_id" IntegerType, Column "description" StringType, Column "is_completed" BoolType] 
                                        [[IntegerValue 601, StringValue "Setup project environment", BoolValue False], 
                                         [IntegerValue 602, StringValue "Complete initial documentation", BoolValue True],
                                         [IntegerValue 605, StringValue "Prepare for deployment", BoolValue True]])
        result <- runExecuteIOTest $ Lib3.executeSql query
        result `shouldBe` expected
    
    

