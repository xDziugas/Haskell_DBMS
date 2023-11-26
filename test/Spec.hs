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
    it "correctly joins tables in the where clause" $ do
      let query = "select name, surname, namee, surnamee from employees, employees2 where id = idd;"
      let expected = Right (DataFrame [Column "name" StringType, Column "surname" StringType, Column "namee" StringType, Column "surnamee" StringType]
                                      [])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly chooses all from table" $ do
      let query = "select * from employees;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly executes where clause" $ do
      let query = "select id from employees where id > 1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [ [IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- runExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
    it "correctly doesn't execute invalid tables" $ do
      let query = "select id from invalid1;"
      let expected = Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
                                       [ [IntegerValue 1, StringValue "Vi", StringValue "Po"]])
      result <- rnExecuteIOTest $ Lib3.executeSql query
      result `shouldBe` expected
      

